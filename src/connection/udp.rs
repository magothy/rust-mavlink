use crate::connection::MavConnection;
use crate::{read_versioned_msg, write_versioned_msg, MavHeader, MavlinkVersion, Message};
use std::collections::HashMap;
use std::io::Read;
use std::io::{self};
use std::net::ToSocketAddrs;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::Mutex;
use std::time;

/// UDP MAVLink connection

pub fn select_protocol<M: Message>(
    address: &str,
) -> io::Result<Box<dyn MavConnection<M> + Sync + Send>> {
    let connection = if let Some(address) = address.strip_prefix("udpin:") {
        udpin(address)
    } else if let Some(address) = address.strip_prefix("udpout:") {
        udpout(address)
    } else if let Some(address) = address.strip_prefix("udpbcast:") {
        udpbcast(address)
    } else if let Some(address) = address.strip_prefix("udpmulti:") {
        udpmulti(address)
    } else {
        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "Protocol unsupported",
        ))
    };

    Ok(Box::new(connection?))
}

pub fn udpbcast<T: ToSocketAddrs>(address: T) -> io::Result<UdpConnection> {
    let addr = address
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("Invalid address");
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket
        .set_broadcast(true)
        .expect("Couldn't bind to broadcast address.");
    UdpConnection::new(socket, false, Some(addr), None)
}

pub fn udpmulti<T: ToSocketAddrs>(address: T) -> io::Result<UdpConnection> {
    let addr = address
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("Invalid address");
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket
        .set_broadcast(true)
        .expect("Couldn't bind to broadcast address.");

    UdpConnection::new(socket, true, None, Some(addr))
}

pub fn udpout<T: ToSocketAddrs>(address: T) -> io::Result<UdpConnection> {
    let addr = address
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("Invalid address");
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    UdpConnection::new(socket, false, Some(addr), None)
}

pub fn udpin<T: ToSocketAddrs>(address: T) -> io::Result<UdpConnection> {
    let addr = address
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("Invalid address");
    let socket = UdpSocket::bind(addr)?;
    UdpConnection::new(socket, true, None, None)
}

struct UdpWrite {
    socket: Arc<UdpSocket>,
    dest: HashMap<SocketAddr, std::time::Instant>,
    heartbeat_dest: Option<SocketAddr>,
    sequence: u8,
}

struct PacketBuf {
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl PacketBuf {
    pub fn new() -> Self {
        Self {
            buf: vec![0; 65536],
            start: 0,
            end: 0,
        }
    }

    pub fn reset(&mut self) -> &mut [u8] {
        self.start = 0;
        self.end = 0;
        &mut self.buf
    }

    pub fn set_len(&mut self, size: usize) {
        self.end = size;
    }

    pub fn slice(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    pub fn len(&self) -> usize {
        self.slice().len()
    }
}

impl Read for PacketBuf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = Read::read(&mut self.slice(), buf)?;
        self.start += n;
        Ok(n)
    }
}

struct UdpRead {
    socket: Arc<UdpSocket>,
    recv_buf: PacketBuf,
}

pub struct UdpConnection {
    reader: Mutex<UdpRead>,
    writer: Mutex<UdpWrite>,
    protocol_version: MavlinkVersion,
    server: bool,
}

impl UdpConnection {
    fn new(
        socket: UdpSocket,
        server: bool,
        dest: Option<SocketAddr>,
        heartbeat_dest: Option<SocketAddr>,
    ) -> io::Result<Self> {
        let mut endpoints = HashMap::new();
        if let Some(dest) = dest {
            endpoints.insert(
                dest,
                time::Instant::now() + time::Duration::from_secs(3600 * 24 * 365 * 100),
            );
        }

        let socket = Arc::new(socket);

        Ok(Self {
            server,
            reader: Mutex::new(UdpRead {
                socket: socket.clone(),
                recv_buf: PacketBuf::new(),
            }),
            writer: Mutex::new(UdpWrite {
                socket,
                dest: endpoints,
                heartbeat_dest,
                sequence: 0,
            }),
            protocol_version: MavlinkVersion::V2,
        })
    }
}

impl<M: Message> MavConnection<M> for UdpConnection {
    fn recv(&self) -> Result<(MavHeader, M), crate::error::MessageReadError> {
        let mut guard = self.reader.lock().unwrap();
        let state = &mut *guard;
        loop {
            if state.recv_buf.len() == 0 {
                let (len, src) = state.socket.recv_from(state.recv_buf.reset())?;
                state.recv_buf.set_len(len);

                if self.server {
                    self.writer
                        .lock()
                        .unwrap()
                        .dest
                        .insert(src, time::Instant::now());
                }
            }

            if let ok @ Ok(..) = read_versioned_msg(&mut state.recv_buf, self.protocol_version) {
                return ok;
            }
        }
    }

    fn send(&self, header: &MavHeader, data: &M) -> Result<usize, crate::error::MessageWriteError> {
        let mut guard = self.writer.lock().unwrap();
        let state = &mut *guard;

        let header = MavHeader {
            sequence: state.sequence,
            system_id: header.system_id,
            component_id: header.component_id,
        };

        state.sequence = state.sequence.wrapping_add(1);

        let mut buf = Vec::new();
        write_versioned_msg(&mut buf, self.protocol_version, header, data)?;

        // remove stale destinations
        state
            .dest
            .retain(|_, time| time.elapsed() < time::Duration::from_secs(15));

        // send to all destinations
        for (addr, _time) in state.dest.iter() {
            state.socket.send_to(&buf, addr)?;
        }

        if let Some(heartbeat_dest) = &state.heartbeat_dest {
            if data.message_id() == 0 {
                state.socket.send_to(&buf, heartbeat_dest)?;
            }
        }

        Ok(buf.len())
    }

    fn set_protocol_version(&mut self, version: MavlinkVersion) {
        self.protocol_version = version;
    }

    fn get_protocol_version(&self) -> MavlinkVersion {
        self.protocol_version
    }
}
