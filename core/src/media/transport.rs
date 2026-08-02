use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use crate::media::packet::{decode_packet, encode_packet, AudioPacket};

const MAX_DATAGRAM: usize = 65_507;

/// Thin UDP wrapper for the media plane.
pub struct MediaSocket {
    socket: UdpSocket,
    addr: SocketAddr,
}

impl MediaSocket {
    pub fn bind() -> Result<Self, String> {
        let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))
            .map_err(|e| format!("no se pudo abrir puerto de media: {e}"))?;
        let addr = socket.local_addr().map_err(|e| e.to_string())?;
        Ok(Self { socket, addr })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn try_clone(&self) -> Result<Self, String> {
        let socket = self.socket.try_clone().map_err(|e| e.to_string())?;
        Ok(Self { socket, addr: self.addr })
    }

    pub fn send_packet(&self, pkt: &AudioPacket, dest: SocketAddr) -> Result<usize, String> {
        let bytes = encode_packet(pkt)?;
        if bytes.len() > MAX_DATAGRAM {
            return Err("paquete de audio demasiado grande".to_string());
        }
        self.socket.send_to(&bytes, dest).map_err(|e| e.to_string())
    }

    /// Blocking receive with a timeout. Returns `Ok(None)` on timeout.
    pub fn recv_packet_timeout(&self, timeout: Duration) -> Result<Option<(AudioPacket, SocketAddr)>, String> {
        self.socket
            .set_read_timeout(Some(timeout))
            .map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; MAX_DATAGRAM];
        match self.socket.recv_from(&mut buf) {
            Ok((len, src)) => {
                let pkt = decode_packet(&buf[..len])?;
                Ok(Some((pkt, src)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                Ok(None)
            }
            Err(e) => Err(format!("recv media: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::{CHANNELS, FRAME_SAMPLES, SAMPLE_RATE};

    #[test]
    fn udp_roundtrip() {
        let a = MediaSocket::bind().unwrap();
        let b = MediaSocket::bind().unwrap();
        let pkt = AudioPacket::new(
            3,
            99,
            5,
            123,
            vec![7i16; FRAME_SAMPLES * CHANNELS as usize],
            SAMPLE_RATE,
            CHANNELS,
        );
        // A socket bound to 0.0.0.0 reports 0.0.0.0 as its local addr; send to a
        // concrete loopback IP like a peer would via its advertised address.
        let dest = SocketAddr::from(([127, 0, 0, 1], b.local_addr().port()));
        a.send_packet(&pkt, dest).unwrap();
        let (recv, src) = b.recv_packet_timeout(Duration::from_millis(500)).unwrap().unwrap();
        assert_eq!(recv, pkt);
        assert_eq!(src.port(), a.local_addr().port());
    }
}
