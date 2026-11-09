//! Continuous mDNS discovery of Cast devices and groups.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc::Sender;

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};

use crate::Event;

const SERVICE: &str = "_googlecast._tcp.local.";

#[derive(Debug, Clone, PartialEq)]
pub struct Device {
    /// Stable identifier of the device or group.
    pub key: String,
    /// The name the user gave it ("Sala").
    pub name: String,
    pub address: SocketAddr,
}

/// Browses forever and reports every device found or lost.
/// Returns when the receiving side of `events` is gone.
pub fn run(events: Sender<Event>) -> Result<()> {
    let mdns = ServiceDaemon::new().context("start mDNS")?;
    let found = mdns.browse(SERVICE).context("browse mDNS")?;
    // mDNS reports a loss by service name: remember which key it belongs to.
    let mut keys: HashMap<String, String> = HashMap::new();

    for event in found {
        let event = match event {
            ServiceEvent::ServiceResolved(info) => {
                // The stream URL is IPv4, and a link-local IPv6 address needs a
                // scope the device cannot use: wait for the IPv4 answer.
                let Some(ip) = info.addresses.iter().map(|a| a.to_ip_addr()).find(IpAddr::is_ipv4)
                else {
                    continue;
                };
                // The key is part of the stream URL: keep only URL-safe characters.
                let mut key = info.get_property_val_str("id").unwrap_or(&info.fullname).to_string();
                key.retain(|c| c.is_ascii_alphanumeric() || c == '-');
                let name = info.get_property_val_str("fn").unwrap_or(&info.fullname).to_string();
                keys.insert(info.fullname.clone(), key.clone());
                Event::Found(Device { key, name, address: SocketAddr::new(ip, info.port) })
            }
            ServiceEvent::ServiceRemoved(_, fullname) => match keys.remove(&fullname) {
                Some(key) => Event::Lost(key),
                None => continue,
            },
            _ => continue,
        };
        if events.send(event).is_err() {
            break;
        }
    }
    Ok(())
}
