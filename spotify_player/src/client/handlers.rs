use std::time::{Duration, Instant};

use anyhow::Context;
use rspotify::model::Id;
use tracing::Instrument;

use crate::{
    config,
    state::{ContextId, ContextPageType, ContextPageUIState, PageState, PlayableId, SharedState},
};

use crate::utils::map_join;

use super::ClientRequest;

struct PlayerEventHandlerState {
    ended_playable_uri: Option<String>,
    last_get_context: Option<(String, Instant, u32)>,
    last_playback_refresh: Instant,
    last_queue_refresh: Option<(String, Instant, u32)>,
}

/// starts the client's request handler
pub async fn start_client_handler(
    state: &SharedState,
    client: &super::AppClient,
    client_sub: &flume::Receiver<ClientRequest>,
) {
    while let Ok(request) = client_sub.recv_async().await {
        let state = state.clone();
        let client = client.clone();
        let span = tracing::info_span!("client_request", request = ?request);

        tokio::task::spawn(
            async move {
                if let Err(err) = client.handle_request(&state, request).await {
                    if client.is_session_invalid().await {
                        tracing::debug!(
                            "Failed to handle client request while session is invalid: {err:#}"
                        );
                    } else {
                        tracing::error!("Failed to handle client request: {err:#}");
                    }
                }
            }
            .instrument(span),
        );
    }
}

/// Interval between background session-validity checks.
const SESSION_CHECK_INTERVAL: Duration = Duration::from_secs(2);
const SESSION_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);
const CONTEXT_REFRESH_THROTTLE: Duration = Duration::from_secs(5);
const QUEUE_REFRESH_THROTTLE: Duration = Duration::from_secs(5);

pub async fn start_session_watcher(state: SharedState, client: super::AppClient) {
    let mut current_interval = SESSION_CHECK_INTERVAL;
    let mut failure_count = 0usize;

    loop {
        tokio::time::sleep(current_interval).await;

        if !client.is_session_invalid().await {
            if failure_count > 0 {
                tracing::info!(
                    "Spotify session restored after {failure_count} failed reconnect attempts"
                );
                failure_count = 0;
                current_interval = SESSION_CHECK_INTERVAL;
            }
            continue;
        }

        failure_count += 1;
        if failure_count == 1 {
            tracing::warn!("Spotify session is invalid, attempting to reconnect...");
        } else if failure_count.is_multiple_of(10) {
            tracing::warn!(
                "Spotify session still disconnected after {failure_count} reconnect attempts, retrying..."
            );
        }

        match client.new_session(Some(&state), false).await {
            Ok(()) => {
                tracing::info!(
                    "Spotify session successfully reconnected after {failure_count} attempts"
                );
                failure_count = 0;
                current_interval = SESSION_CHECK_INTERVAL;
            }
            Err(err) => {
                if failure_count == 1 {
                    tracing::error!("Failed to reconnect client session: {err:#}");
                } else {
                    tracing::debug!(
                        "Failed to reconnect client session (attempt {failure_count}): {err:#}"
                    );
                }
                let shift = failure_count.min(5) as u32;
                current_interval = std::cmp::min(
                    SESSION_CHECK_INTERVAL * 2u32.saturating_pow(shift),
                    SESSION_RECONNECT_MAX_BACKOFF,
                );
            }
        }
    }
}

fn handle_playback_change_event(
    state: &SharedState,
    client_pub: &flume::Sender<ClientRequest>,
    handler_state: &mut PlayerEventHandlerState,
) -> anyhow::Result<()> {
    let player = state.player.read();
    let (playback, id, duration) = match (
        player.buffered_playback.as_ref(),
        player.currently_playing(),
    ) {
        (Some(playback), Some(rspotify::model::PlayableItem::Track(track))) => (
            playback,
            PlayableId::Track(track.id.clone().expect("null track_id")),
            track.duration,
        ),
        (Some(playback), Some(rspotify::model::PlayableItem::Episode(episode))) => (
            playback,
            PlayableId::Episode(episode.id.clone()),
            episode.duration,
        ),
        _ => return Ok(()),
    };
    let playable_uri = id.uri();

    let playback_ended = player
        .playback_progress()
        .is_some_and(|progress| progress >= duration && playback.is_playing);
    if playback_ended && handler_state.ended_playable_uri.as_deref() != Some(&playable_uri) {
        client_pub.send(ClientRequest::GetCurrentPlayback)?;
        handler_state.ended_playable_uri = Some(playable_uri.clone());
    } else if !playback_ended {
        handler_state.ended_playable_uri = None;
    }

    let queue_needs_refresh = player.queue.as_ref().is_none_or(|queue| {
        queue
            .currently_playing
            .as_ref()
            .is_none_or(|queue_item| queue_item.id().expect("null track_id") != id)
    });
    if queue_needs_refresh {
        let should_refresh = match &handler_state.last_queue_refresh {
            Some((last_uri, timer, attempts)) if last_uri == &playable_uri => {
                let shift = (*attempts).min(4);
                let throttle = QUEUE_REFRESH_THROTTLE * 2u32.saturating_pow(shift);
                timer.elapsed() >= throttle
            }
            _ => true,
        };
        if should_refresh {
            let next_attempts = match &handler_state.last_queue_refresh {
                Some((last_uri, _, attempts)) if last_uri == &playable_uri => attempts + 1,
                _ => 0,
            };
            handler_state.last_queue_refresh = Some((playable_uri, Instant::now(), next_attempts));
            client_pub.send(ClientRequest::GetCurrentUserQueue)?;
        }
    } else {
        handler_state.last_queue_refresh = None;
    }

    Ok(())
}

fn handle_page_change_event(
    state: &SharedState,
    client_pub: &flume::Sender<ClientRequest>,
    handler_state: &mut PlayerEventHandlerState,
) -> anyhow::Result<()> {
    match state.ui.lock().current_page_mut() {
        PageState::Context {
            id,
            context_page_type,
            state: page_state,
        } => {
            let expected_id = match context_page_type {
                ContextPageType::Browsing(context_id) => Some(context_id.clone()),
                ContextPageType::CurrentPlaying => state.player.read().playing_context_id(),
            };

            let new_id = if *id == expected_id {
                false
            } else {
                // update the context state and request new data when moving to a new context page
                tracing::info!("Current context ID ({:?}) is different from the expected ID ({:?}), update the context state", id, expected_id);

                *id = expected_id;

                // update the UI page state based on the context's type
                match id {
                    Some(id) => {
                        *page_state = Some(match id {
                            ContextId::Album(_) => ContextPageUIState::new_album(),
                            ContextId::Artist(_) => ContextPageUIState::new_artist(),
                            ContextId::Playlist(_) => ContextPageUIState::new_playlist(),
                            ContextId::Tracks(_) => ContextPageUIState::new_tracks(),
                            ContextId::Show(_) => ContextPageUIState::new_show(),
                        });
                    }
                    None => {
                        *page_state = None;
                    }
                }
                true
            };

            // request new context's data if not found in memory
            // To avoid making too many requests, only request if context id is changed
            // or it's been a while since the last request.
            if let Some(id) = id {
                let context_uri = id.uri();
                let is_tracks = matches!(id, ContextId::Tracks(_));
                let (is_cached, is_in_flight) = {
                    let data = state.data.read();
                    (
                        data.caches.context.contains_key(&context_uri),
                        data.caches.context_in_flight.contains(&context_uri),
                    )
                };

                if !is_tracks && !is_cached && !is_in_flight {
                    let should_request = if new_id {
                        true
                    } else {
                        match &handler_state.last_get_context {
                            Some((last_uri, timer, attempts)) if last_uri == &context_uri => {
                                let shift = (*attempts).min(4);
                                let throttle =
                                    CONTEXT_REFRESH_THROTTLE * 2u32.saturating_pow(shift);
                                timer.elapsed() >= throttle
                            }
                            _ => true,
                        }
                    };

                    if should_request {
                        let next_attempts = if new_id {
                            0
                        } else {
                            match &handler_state.last_get_context {
                                Some((last_uri, _, attempts)) if last_uri == &context_uri => {
                                    attempts + 1
                                }
                                _ => 0,
                            }
                        };
                        handler_state.last_get_context =
                            Some((context_uri, Instant::now(), next_attempts));
                        client_pub.send(ClientRequest::GetContext(id.clone()))?;
                    }
                } else if is_cached {
                    handler_state.last_get_context = None;
                }
            }
        }

        PageState::Lyrics {
            track_uri,
            track,
            artists,
        } => {
            if let Some(rspotify::model::PlayableItem::Track(current_track)) =
                state.player.read().currently_playing()
            {
                if current_track.name != *track {
                    if let Some(id) = &current_track.id {
                        tracing::info!("Currently playing track \"{}\" is different from the track \"{track}\" shown up in the lyrics page. Fetching new track's lyrics...", current_track.name);
                        track.clone_from(&current_track.name);
                        *artists = map_join(&current_track.artists, |a| &a.name, ", ");
                        *track_uri = id.uri();
                        client_pub.send(ClientRequest::GetLyrics {
                            track_id: id.clone_static(),
                        })?;
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

fn handle_player_event(
    state: &SharedState,
    client_pub: &flume::Sender<ClientRequest>,
    handler_state: &mut PlayerEventHandlerState,
) -> anyhow::Result<()> {
    handle_page_change_event(state, client_pub, handler_state)
        .context("handle page change event")?;
    handle_playback_change_event(state, client_pub, handler_state)
        .context("handle playback change event")?;

    Ok(())
}

/// Starts event watcher listening to events and making update requests to the client if needed
pub fn start_player_event_watcher(state: &SharedState, client_pub: &flume::Sender<ClientRequest>) {
    let configs = config::get_config();

    let refresh_duration = Duration::from_millis(100);
    let playback_refresh_duration =
        Duration::from_millis(configs.app_config.playback_refresh_duration_in_ms);
    let mut handler_state = PlayerEventHandlerState {
        last_get_context: None,
        last_playback_refresh: Instant::now(),
        ended_playable_uri: None,
        last_queue_refresh: None,
    };

    loop {
        // periodically refresh the playback state (if enabled in config)
        if configs.app_config.playback_refresh_duration_in_ms > 0
            && handler_state.last_playback_refresh.elapsed() >= playback_refresh_duration
        {
            client_pub
                .send(ClientRequest::GetCurrentPlayback)
                .unwrap_or_default();
            handler_state.last_playback_refresh = Instant::now();
        }

        if let Err(err) = handle_player_event(state, client_pub, &mut handler_state) {
            tracing::error!("Encounter error when handling player event: {err:#}");
        }

        std::thread::sleep(refresh_duration);
    }
}
