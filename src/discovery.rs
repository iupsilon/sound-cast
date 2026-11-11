//! Continuous mDNS discovery of Cast devices and groups, and the devices given
//! by hand with `--device`.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use mdns_sd::{ServiceDaemon, ServiceEvent};

use crate::Event;

const SERVICE: &str = "_googlecast._tcp.local.";
/// The CASTV2 port of a Cast device.
pub const CAST_PORT: u16 = 8009;
/// How often the browse loop looks for a rescan request.
const RESCAN_POLL: Duration = Duration::from_millis(500);

/// Bits of the `ca` TXT record: what the device can do.
const CAPABILITY_VIDEO_OUT: u32 = 1 << 0;
const CAPABILITY_GROUP: u32 = 1 << 5;

#[derive(Debug, Clone, PartialEq)]
pub struct Device {
    /// Stable identifier of the device or group.
    pub key: String,
    /// The name the user gave it ("Sala"), or its `--alias`.
    pub name: String,
    pub address: SocketAddr,
    /// The model ("Google Home Mini", "Google Cast Group"), empty if unknown.
    pub model: String,
    /// True when the device has a screen (a TV, a Nest Hub).
    pub screen: bool,
    /// True for a speaker group.
    pub group: bool,
    /// True for a device given with `--device`: mDNS never reports it lost.
    pub fixed: bool,
}

/// A device given by hand: `NAME=HOST[:PORT]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Fixed {
    pub name: String,
    pub host: String,
    pub port: u16,
}

impl Fixed {
    pub fn parse(text: &str) -> Result<Self> {
        let (name, address) =
            text.split_once('=').ok_or(anyhow!("--device needs NAME=HOST[:PORT], got {text}"))?;
        let name = name.trim();
        if name.is_empty() {
            bail!("--device {text}: the name is empty");
        }
        // An IPv6 address goes in brackets when it has a port: [fd00::1]:8009.
        let (host, port) = match address.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') || host.ends_with(']') => {
                (host, port.parse().with_context(|| format!("--device {text}: port"))?)
            }
            _ => (address, CAST_PORT),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            bail!("--device {text}: the host is empty");
        }
        Ok(Self { name: name.to_string(), host: host.to_string(), port })
    }

    /// Resolves the host. IPv4 first and no link-local IPv6, as for the devices
    /// found by mDNS.
    pub fn device(&self) -> Result<Device> {
        let addresses: Vec<SocketAddr> = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .with_context(|| format!("resolve {}", self.host))?
            .collect();
        let ip = usable_address(addresses.iter().map(SocketAddr::ip))
            .ok_or(anyhow!("{} has no usable address", self.host))?;
        let address = SocketAddr::new(ip, self.port);
        let mut key: String = self
            .name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
            .collect();
        key = format!("fixed-{}", key.trim_matches('-'));
        Ok(Device {
            key,
            name: self.name.clone(),
            address,
            model: String::new(),
            screen: false,
            group: false,
            fixed: true,
        })
    }
}

/// The address to reach a device at. IPv4 is preferred; an IPv6 address must be
/// routable, since a link-local one needs a scope the device cannot put in a URL.
fn usable_address(addresses: impl Iterator<Item = IpAddr>) -> Option<IpAddr> {
    let addresses: Vec<IpAddr> = addresses.collect();
    addresses.iter().copied().find(IpAddr::is_ipv4).or_else(|| {
        addresses.iter().copied().find(|ip| match ip {
            IpAddr::V6(v6) => !v6.is_unicast_link_local() && !v6.is_loopback(),
            IpAddr::V4(_) => false,
        })
    })
}

/// Browses forever and reports every device found or lost. A message on
/// `rescan` starts the browse again, so every device answers anew.
/// Returns when the receiving side of `events` is gone, and fails when mDNS stops.
pub fn run(events: Sender<Event>, rescan: Receiver<()>) -> Result<()> {
    let mdns = ServiceDaemon::new().context("start mDNS")?;
    let mut found = mdns.browse(SERVICE).context("browse mDNS")?;
    // mDNS reports a loss by service name: remember which key it belongs to.
    let mut keys: HashMap<String, String> = HashMap::new();

    loop {
        if rescan.try_iter().count() > 0 {
            eprintln!("discovery: rescan");
            let _ = mdns.stop_browse(SERVICE);
            found = mdns.browse(SERVICE).context("browse mDNS")?;
        }
        let event = match found.recv_timeout(RESCAN_POLL) {
            Ok(event) => event,
            Err(mdns_sd::RecvTimeoutError::Timeout) => continue,
            Err(mdns_sd::RecvTimeoutError::Disconnected) => bail!("mDNS browsing stopped"),
        };
        let event = match event {
            ServiceEvent::ServiceResolved(info) => {
                // Wait for an answer with an address the device can use in a URL.
                let Some(ip) = usable_address(info.addresses.iter().map(|a| a.to_ip_addr())) else {
                    continue;
                };
                // The key is part of the stream URL: keep only URL-safe characters.
                let mut key = info.get_property_val_str("id").unwrap_or(&info.fullname).to_string();
                key.retain(|c| c.is_ascii_alphanumeric() || c == '-');
                let name = info.get_property_val_str("fn").unwrap_or(&info.fullname).to_string();
                let model = info.get_property_val_str("md").unwrap_or_default().to_string();
                let capabilities: u32 =
                    info.get_property_val_str("ca").and_then(|c| c.parse().ok()).unwrap_or(0);
                keys.insert(info.fullname.clone(), key.clone());
                Event::Found(Device {
                    key,
                    name,
                    address: SocketAddr::new(ip, info.port),
                    group: capabilities & CAPABILITY_GROUP != 0 || model == "Google Cast Group",
                    screen: capabilities & CAPABILITY_VIDEO_OUT != 0,
                    model,
                    fixed: false,
                })
            }
            ServiceEvent::ServiceRemoved(_, fullname) => match keys.remove(&fullname) {
                Some(key) => Event::Lost(key),
                None => continue,
            },
            _ => continue,
        };
        if events.send(event).is_err() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_devices_parse() {
        let fixed = Fixed::parse("Studio=192.168.1.40").unwrap();
        assert_eq!(
            (fixed.name.as_str(), fixed.host.as_str(), fixed.port),
            ("Studio", "192.168.1.40", 8009)
        );
        let fixed = Fixed::parse("Studio=speaker.lan:8010").unwrap();
        assert_eq!((fixed.host.as_str(), fixed.port), ("speaker.lan", 8010));
        let fixed = Fixed::parse("TV=[fd00::5]:8009").unwrap();
        assert_eq!((fixed.host.as_str(), fixed.port), ("fd00::5", 8009));
        let fixed = Fixed::parse("TV=fd00::5").unwrap();
        assert_eq!((fixed.host.as_str(), fixed.port), ("fd00::5", 8009));
        assert!(Fixed::parse("192.168.1.40").is_err());
        assert!(Fixed::parse("=192.168.1.40").is_err());
    }

    #[test]
    fn fixed_device_key_is_url_safe() {
        let device = Fixed::parse("Casa di Però=127.0.0.1").unwrap().device().unwrap();
        assert_eq!(device.key, "fixed-casa-di-per");
        assert!(device.fixed);
    }

    #[test]
    fn ipv4_is_preferred_and_link_local_skipped() {
        let v4: IpAddr = "192.168.1.5".parse().unwrap();
        let link: IpAddr = "fe80::1".parse().unwrap();
        let global: IpAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(usable_address([link, global, v4].into_iter()), Some(v4));
        assert_eq!(usable_address([link, global].into_iter()), Some(global));
        assert_eq!(usable_address([link].into_iter()), None);
    }
}
