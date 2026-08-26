#![allow(dead_code)]

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CodecError {
    Incomplete,
    InvalidVarint,
    MalformedPacket,
    ProtocolError,
    PacketTooLarge,
    UnsupportedProtocolVersion(u8),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Incomplete => write!(f, "Incomplete packet"),
            CodecError::InvalidVarint => write!(f, "Invalid variable byte integer"),
            CodecError::MalformedPacket => write!(f, "Malformed MQTT packet"),
            CodecError::ProtocolError => write!(f, "MQTT protocol error"),
            CodecError::PacketTooLarge => {
                write!(f, "MQTT packet exceeds negotiated Maximum Packet Size")
            }
            CodecError::UnsupportedProtocolVersion(v) => {
                write!(f, "Unsupported protocol version: {}", v)
            }
        }
    }
}

impl std::error::Error for CodecError {}

// Helper functions for reading primitive data types from byte slices

#[inline]
pub fn read_u8(buf: &mut &[u8]) -> Result<u8, CodecError> {
    if buf.is_empty() {
        return Err(CodecError::Incomplete);
    }
    let val = buf[0];
    *buf = &buf[1..];
    Ok(val)
}

#[inline]
pub fn read_u16(buf: &mut &[u8]) -> Result<u16, CodecError> {
    if buf.len() < 2 {
        return Err(CodecError::Incomplete);
    }
    let val = u16::from_be_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    Ok(val)
}

#[inline]
pub fn read_u32(buf: &mut &[u8]) -> Result<u32, CodecError> {
    if buf.len() < 4 {
        return Err(CodecError::Incomplete);
    }
    let val = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    Ok(val)
}

#[inline]
pub fn read_str<'a>(buf: &mut &'a [u8]) -> Result<&'a str, CodecError> {
    let len = read_u16(buf)? as usize;
    if buf.len() < len {
        return Err(CodecError::Incomplete);
    }
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| CodecError::MalformedPacket)?;
    for ch in s.chars() {
        let cp = ch as u32;
        let disallowed_control = cp == 0 || (1..=0x1f).contains(&cp) || (0x7f..=0x9f).contains(&cp);
        let noncharacter =
            (0xfdd0..=0xfdef).contains(&cp) || (cp & 0xffff == 0xfffe) || (cp & 0xffff == 0xffff);
        if disallowed_control || noncharacter {
            return Err(CodecError::MalformedPacket);
        }
    }
    *buf = &buf[len..];
    Ok(s)
}

#[inline]
fn read_utf8_str_structural<'a>(buf: &mut &'a [u8]) -> Result<&'a str, CodecError> {
    let len = read_u16(buf)? as usize;
    if buf.len() < len {
        return Err(CodecError::Incomplete);
    }
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| CodecError::MalformedPacket)?;
    *buf = &buf[len..];
    Ok(s)
}

#[inline]
pub fn read_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], CodecError> {
    let len = read_u16(buf)? as usize;
    if buf.len() < len {
        return Err(CodecError::Incomplete);
    }
    let data = &buf[..len];
    *buf = &buf[len..];
    Ok(data)
}

/// Decodes a Variable Byte Integer (Varint) from a byte slice.
/// Returns Ok((value, bytes_read)) on success.
pub fn decode_varint(buf: &[u8]) -> Result<(u32, usize), CodecError> {
    let mut multiplier: u32 = 1;
    let mut value: u32 = 0;
    let mut bytes_read = 0;

    for &byte in buf {
        bytes_read += 1;
        value += ((byte & 127) as u32) * multiplier;

        if (byte & 128) == 0 {
            let minimum = match bytes_read {
                1 => 0,
                2 => 128,
                3 => 16_384,
                4 => 2_097_152,
                _ => return Err(CodecError::InvalidVarint),
            };
            if value < minimum {
                return Err(CodecError::InvalidVarint);
            }
            return Ok((value, bytes_read));
        }

        // Max value is 268,435,455 (4 bytes max)
        if multiplier >= 128 * 128 * 128 {
            return Err(CodecError::InvalidVarint);
        }
        multiplier *= 128;
    }

    Err(CodecError::Incomplete)
}

/// Returns the full MQTT Control Packet size as soon as the fixed header is complete.
/// `Ok(None)` means more bytes are needed to finish the Remaining Length field.
pub fn packet_total_len(buf: &[u8]) -> Result<Option<usize>, CodecError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    match decode_varint(&buf[1..]) {
        Ok((remaining, used)) => Ok(Some(1 + used + remaining as usize)),
        Err(CodecError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Encodes a u32 value as a Variable Byte Integer (Varint) into a vector.
pub fn encode_varint(mut value: u32, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 128;
            buf.push(byte);
        } else {
            buf.push(byte);
            break;
        }
    }
}

// Helper functions for writing primitive types
#[inline]
pub fn write_u16(val: u16, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&val.to_be_bytes());
}

#[inline]
pub fn write_u32(val: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&val.to_be_bytes());
}

#[inline]
pub fn write_str(s: &str, buf: &mut Vec<u8>) {
    write_u16(s.len() as u16, buf);
    buf.extend_from_slice(s.as_bytes());
}

#[inline]
pub fn write_bytes(data: &[u8], buf: &mut Vec<u8>) {
    write_u16(data.len() as u16, buf);
    buf.extend_from_slice(data);
}

// Packet Enums and Structures

#[derive(Debug, PartialEq, Eq)]
pub enum Packet<'a> {
    Connect(Connect<'a>),
    ConnAck(ConnAck<'a>),
    Publish(Publish<'a>),
    PubAck(PubAck<'a>),
    PubRec(PubRec<'a>),
    PubRel(PubRel<'a>),
    PubComp(PubComp<'a>),
    Subscribe(Subscribe<'a>),
    SubAck(SubAck<'a>),
    Unsubscribe(Unsubscribe<'a>),
    UnsubAck(UnsubAck<'a>),
    PingReq,
    PingResp,
    Disconnect(Disconnect<'a>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Connect<'a> {
    pub client_id: &'a str,
    pub keep_alive: u16,
    pub clean_start: bool,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
    pub will: Option<Will<'a>>,
    pub properties: ConnectProperties<'a>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConnectProperties<'a> {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    pub user_properties: Vec<(&'a str, &'a str)>,
    pub auth_method: Option<&'a str>,
    pub auth_data: Option<&'a [u8]>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Will<'a> {
    pub qos: u8,
    pub retain: bool,
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub properties: WillProperties<'a>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct WillProperties<'a> {
    pub will_delay_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<&'a str>,
    pub response_topic: Option<&'a str>,
    pub correlation_data: Option<&'a [u8]>,
    pub user_properties: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConnAck<'a> {
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: ConnAckProperties<'a>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConnAckProperties<'a> {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<&'a str>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<&'a str>,
    pub user_properties: Vec<(&'a str, &'a str)>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifiers_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<&'a str>,
    pub server_reference: Option<&'a str>,
    pub authentication_method: Option<&'a str>,
    pub authentication_data: Option<&'a [u8]>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: &'a str,
    pub packet_id: Option<u16>, // Required if QoS > 0
    pub properties: PublishProperties<'a>,
    pub payload: &'a [u8],
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PublishProperties<'a> {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<&'a str>,
    pub response_topic: Option<&'a str>,
    pub correlation_data: Option<&'a [u8]>,
    pub subscription_identifiers: Vec<u32>, // Varint; may repeat Server -> Client
    pub topic_alias: Option<u16>,
    pub user_properties: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PubAck<'a> {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: PubAckProperties<'a>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PubAckProperties<'a> {
    pub reason_string: Option<&'a str>,
    pub user_properties: Vec<(&'a str, &'a str)>,
}

pub type PubRec<'a> = PubAck<'a>;
pub type PubRel<'a> = PubAck<'a>;
pub type PubComp<'a> = PubAck<'a>;

#[derive(Debug, PartialEq, Eq)]
pub struct Subscribe<'a> {
    pub packet_id: u16,
    pub properties: SubscribeProperties<'a>,
    pub subscriptions: Vec<Subscription<'a>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SubscribeProperties<'a> {
    pub subscription_identifier: Option<u32>, // Varint
    pub user_properties: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Subscription<'a> {
    pub topic_filter: &'a str,
    pub options: u8, // QoS (bits 0,1), No Local (bit 2), Retain As Publish (bit 3), Retain Handling (bits 4,5)
}

#[derive(Debug, PartialEq, Eq)]
pub struct SubAck<'a> {
    pub packet_id: u16,
    pub properties: SubAckProperties<'a>,
    pub reason_codes: Vec<u8>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SubAckProperties<'a> {
    pub reason_string: Option<&'a str>,
    pub user_properties: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Unsubscribe<'a> {
    pub packet_id: u16,
    pub properties: UnsubscribeProperties<'a>,
    pub topic_filters: Vec<&'a str>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct UnsubscribeProperties<'a> {
    pub user_properties: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct UnsubAck<'a> {
    pub packet_id: u16,
    pub properties: SubAckProperties<'a>,
    pub reason_codes: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Disconnect<'a> {
    pub reason_code: u8,
    pub properties: DisconnectProperties<'a>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DisconnectProperties<'a> {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<&'a str>,
    pub user_properties: Vec<(&'a str, &'a str)>,
    pub server_reference: Option<&'a str>,
}

// Decode logic implementations

impl<'a> ConnectProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = ConnectProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x11 => {
                    if props.session_expiry_interval.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.session_expiry_interval = Some(read_u32(&mut prop_slice)?);
                }
                0x21 => {
                    if props.receive_maximum.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    let value = read_u16(&mut prop_slice)?;
                    if value == 0 {
                        return Err(CodecError::ProtocolError);
                    }
                    props.receive_maximum = Some(value);
                }
                0x27 => {
                    if props.max_packet_size.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    let value = read_u32(&mut prop_slice)?;
                    if value == 0 {
                        return Err(CodecError::ProtocolError);
                    }
                    props.max_packet_size = Some(value);
                }
                0x22 => {
                    if props.topic_alias_maximum.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.topic_alias_maximum = Some(read_u16(&mut prop_slice)?);
                }
                0x19 => {
                    if props.request_response_info.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    let value = read_u8(&mut prop_slice)?;
                    if value > 1 {
                        return Err(CodecError::ProtocolError);
                    }
                    props.request_response_info = Some(value);
                }
                0x17 => {
                    if props.request_problem_info.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    let value = read_u8(&mut prop_slice)?;
                    if value > 1 {
                        return Err(CodecError::ProtocolError);
                    }
                    props.request_problem_info = Some(value);
                }
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                0x15 => {
                    if props.auth_method.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.auth_method = Some(read_str(&mut prop_slice)?);
                }
                0x16 => {
                    if props.auth_data.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.auth_data = Some(read_bytes(&mut prop_slice)?);
                }
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf = Vec::new();
        if let Some(val) = self.session_expiry_interval {
            temp_buf.push(0x11);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.receive_maximum {
            temp_buf.push(0x21);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.max_packet_size {
            temp_buf.push(0x27);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.topic_alias_maximum {
            temp_buf.push(0x22);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.request_response_info {
            temp_buf.push(0x19);
            temp_buf.push(val);
        }
        if let Some(val) = self.request_problem_info {
            temp_buf.push(0x17);
            temp_buf.push(val);
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }
        if let Some(val) = self.auth_method {
            temp_buf.push(0x15);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.auth_data {
            temp_buf.push(0x16);
            write_bytes(val, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

impl<'a> ConnAckProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = ConnAckProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x11 => props.session_expiry_interval = Some(read_u32(&mut prop_slice)?),
                0x21 => props.receive_maximum = Some(read_u16(&mut prop_slice)?),
                0x24 => props.max_qos = Some(read_u8(&mut prop_slice)?),
                0x25 => props.retain_available = Some(read_u8(&mut prop_slice)?),
                0x27 => props.maximum_packet_size = Some(read_u32(&mut prop_slice)?),
                0x12 => props.assigned_client_identifier = Some(read_str(&mut prop_slice)?),
                0x22 => props.topic_alias_maximum = Some(read_u16(&mut prop_slice)?),
                0x1F => props.reason_string = Some(read_str(&mut prop_slice)?),
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                0x28 => props.wildcard_subscription_available = Some(read_u8(&mut prop_slice)?),
                0x29 => props.subscription_identifiers_available = Some(read_u8(&mut prop_slice)?),
                0x2A => props.shared_subscription_available = Some(read_u8(&mut prop_slice)?),
                0x13 => props.server_keep_alive = Some(read_u16(&mut prop_slice)?),
                0x1A => props.response_information = Some(read_str(&mut prop_slice)?),
                0x1C => props.server_reference = Some(read_str(&mut prop_slice)?),
                0x15 => props.authentication_method = Some(read_str(&mut prop_slice)?),
                0x16 => props.authentication_data = Some(read_bytes(&mut prop_slice)?),
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf = Vec::new();
        if let Some(val) = self.session_expiry_interval {
            temp_buf.push(0x11);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.receive_maximum {
            temp_buf.push(0x21);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.max_qos {
            temp_buf.push(0x24);
            temp_buf.push(val);
        }
        if let Some(val) = self.retain_available {
            temp_buf.push(0x25);
            temp_buf.push(val);
        }
        if let Some(val) = self.maximum_packet_size {
            temp_buf.push(0x27);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.assigned_client_identifier {
            temp_buf.push(0x12);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.topic_alias_maximum {
            temp_buf.push(0x22);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.reason_string {
            temp_buf.push(0x1F);
            write_str(val, &mut temp_buf);
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }
        if let Some(val) = self.wildcard_subscription_available {
            temp_buf.push(0x28);
            temp_buf.push(val);
        }
        if let Some(val) = self.subscription_identifiers_available {
            temp_buf.push(0x29);
            temp_buf.push(val);
        }
        if let Some(val) = self.shared_subscription_available {
            temp_buf.push(0x2A);
            temp_buf.push(val);
        }
        if let Some(val) = self.server_keep_alive {
            temp_buf.push(0x13);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.response_information {
            temp_buf.push(0x1A);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.server_reference {
            temp_buf.push(0x1C);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.authentication_method {
            temp_buf.push(0x15);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.authentication_data {
            temp_buf.push(0x16);
            write_bytes(val, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

impl<'a> PublishProperties<'a> {
    pub fn is_empty(&self) -> bool {
        self.payload_format_indicator.is_none()
            && self.message_expiry_interval.is_none()
            && self.content_type.is_none()
            && self.response_topic.is_none()
            && self.correlation_data.is_none()
            && self.subscription_identifiers.is_empty()
            && self.topic_alias.is_none()
            && self.user_properties.is_empty()
    }

    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = PublishProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x01 => {
                    if props.payload_format_indicator.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    let value = read_u8(&mut prop_slice)?;
                    if value > 1 {
                        return Err(CodecError::ProtocolError);
                    }
                    props.payload_format_indicator = Some(value);
                }
                0x02 => {
                    if props.message_expiry_interval.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.message_expiry_interval = Some(read_u32(&mut prop_slice)?);
                }
                0x03 => {
                    if props.content_type.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.content_type = Some(read_str(&mut prop_slice)?);
                }
                0x08 => {
                    if props.response_topic.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.response_topic = Some(read_str(&mut prop_slice)?);
                }
                0x09 => {
                    if props.correlation_data.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.correlation_data = Some(read_bytes(&mut prop_slice)?);
                }
                0x0B => {
                    let (val, read) = decode_varint(prop_slice)?;
                    if val == 0 {
                        return Err(CodecError::ProtocolError);
                    }
                    prop_slice = &prop_slice[read..];
                    props.subscription_identifiers.push(val);
                }
                0x23 => {
                    if props.topic_alias.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.topic_alias = Some(read_u16(&mut prop_slice)?);
                }
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        if self.payload_format_indicator.is_none()
            && self.message_expiry_interval.is_none()
            && self.content_type.is_none()
            && self.response_topic.is_none()
            && self.correlation_data.is_none()
            && self.subscription_identifiers.is_empty()
            && self.topic_alias.is_none()
            && self.user_properties.is_empty()
        {
            buf.push(0); // Property Length = 0
            return;
        }

        let mut temp_buf = Vec::new();
        if let Some(val) = self.payload_format_indicator {
            temp_buf.push(0x01);
            temp_buf.push(val);
        }
        if let Some(val) = self.message_expiry_interval {
            temp_buf.push(0x02);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.content_type {
            temp_buf.push(0x03);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.response_topic {
            temp_buf.push(0x08);
            write_str(val, &mut temp_buf);
        }
        if let Some(val) = self.correlation_data {
            temp_buf.push(0x09);
            write_bytes(val, &mut temp_buf);
        }
        for val in &self.subscription_identifiers {
            temp_buf.push(0x0B);
            encode_varint(*val, &mut temp_buf);
        }
        if let Some(val) = self.topic_alias {
            temp_buf.push(0x23);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

impl<'a> WillProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];
        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }
        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];
        let mut props = WillProperties::default();
        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x18 => {
                    if props.will_delay_interval.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.will_delay_interval = Some(read_u32(&mut prop_slice)?);
                }
                0x01 => {
                    if props.payload_format_indicator.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    let value = read_u8(&mut prop_slice)?;
                    if value > 1 {
                        return Err(CodecError::ProtocolError);
                    }
                    props.payload_format_indicator = Some(value);
                }
                0x02 => {
                    if props.message_expiry_interval.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.message_expiry_interval = Some(read_u32(&mut prop_slice)?);
                }
                0x03 => {
                    if props.content_type.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.content_type = Some(read_str(&mut prop_slice)?);
                }
                0x08 => {
                    if props.response_topic.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.response_topic = Some(read_str(&mut prop_slice)?);
                }
                0x09 => {
                    if props.correlation_data.is_some() {
                        return Err(CodecError::ProtocolError);
                    }
                    props.correlation_data = Some(read_bytes(&mut prop_slice)?);
                }
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let value = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, value));
                }
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut props = Vec::new();
        if let Some(value) = self.will_delay_interval {
            props.push(0x18);
            props.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(value) = self.payload_format_indicator {
            props.push(0x01);
            props.push(value);
        }
        if let Some(value) = self.message_expiry_interval {
            props.push(0x02);
            props.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(value) = self.content_type {
            props.push(0x03);
            write_str(value, &mut props);
        }
        if let Some(value) = self.response_topic {
            props.push(0x08);
            write_str(value, &mut props);
        }
        if let Some(value) = self.correlation_data {
            props.push(0x09);
            write_bytes(value, &mut props);
        }
        for (key, value) in &self.user_properties {
            props.push(0x26);
            write_str(key, &mut props);
            write_str(value, &mut props);
        }
        encode_varint(props.len() as u32, buf);
        buf.extend_from_slice(&props);
    }
}

impl<'a> PubAckProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = PubAckProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x1F => props.reason_string = Some(read_str(&mut prop_slice)?),
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf = Vec::new();
        if let Some(val) = self.reason_string {
            temp_buf.push(0x1F);
            write_str(val, &mut temp_buf);
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

impl<'a> SubscribeProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = SubscribeProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x0B => {
                    let (val, read) = decode_varint(prop_slice)?;
                    prop_slice = &prop_slice[read..];
                    props.subscription_identifier = Some(val);
                }
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf = Vec::new();
        if let Some(val) = self.subscription_identifier {
            temp_buf.push(0x0B);
            encode_varint(val, &mut temp_buf);
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

impl<'a> SubAckProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = SubAckProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x1F => props.reason_string = Some(read_str(&mut prop_slice)?),
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf = Vec::new();
        if let Some(val) = self.reason_string {
            temp_buf.push(0x1F);
            write_str(val, &mut temp_buf);
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

impl<'a> UnsubscribeProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];
        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }
        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];
        let mut props = Self::default();
        while !prop_slice.is_empty() {
            match read_u8(&mut prop_slice)? {
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let value = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, value));
                }
                _ => return Err(CodecError::ProtocolError),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp = Vec::new();
        for (key, value) in &self.user_properties {
            temp.push(0x26);
            write_str(key, &mut temp);
            write_str(value, &mut temp);
        }
        encode_varint(temp.len() as u32, buf);
        buf.extend_from_slice(&temp);
    }
}

impl<'a> DisconnectProperties<'a> {
    pub fn decode(buf: &mut &'a [u8]) -> Result<Self, CodecError> {
        let mut props = DisconnectProperties::default();
        if buf.is_empty() {
            return Ok(props);
        }

        let (prop_len, read) = decode_varint(buf)?;
        *buf = &buf[read..];

        if buf.len() < prop_len as usize {
            return Err(CodecError::Incomplete);
        }

        let mut prop_slice = &buf[..prop_len as usize];
        *buf = &buf[prop_len as usize..];

        while !prop_slice.is_empty() {
            let prop_id = read_u8(&mut prop_slice)?;
            match prop_id {
                0x11 => props.session_expiry_interval = Some(read_u32(&mut prop_slice)?),
                0x1F => props.reason_string = Some(read_str(&mut prop_slice)?),
                0x26 => {
                    let key = read_str(&mut prop_slice)?;
                    let val = read_str(&mut prop_slice)?;
                    props.user_properties.push((key, val));
                }
                0x1C => props.server_reference = Some(read_str(&mut prop_slice)?),
                _ => return Err(CodecError::MalformedPacket),
            }
        }
        Ok(props)
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf = Vec::new();
        if let Some(val) = self.session_expiry_interval {
            temp_buf.push(0x11);
            temp_buf.extend_from_slice(&val.to_be_bytes());
        }
        if let Some(val) = self.reason_string {
            temp_buf.push(0x1F);
            write_str(val, &mut temp_buf);
        }
        for (k, v) in &self.user_properties {
            temp_buf.push(0x26);
            write_str(k, &mut temp_buf);
            write_str(v, &mut temp_buf);
        }
        if let Some(val) = self.server_reference {
            temp_buf.push(0x1C);
            write_str(val, &mut temp_buf);
        }

        encode_varint(temp_buf.len() as u32, buf);
        buf.extend_from_slice(&temp_buf);
    }
}

// Packet decoders

pub fn decode_packet<'a>(raw_buf: &'a [u8]) -> Result<(Packet<'a>, usize), CodecError> {
    decode_packet_limited(raw_buf, usize::MAX)
}

/// Decodes one MQTT Control Packet while enforcing the peer-facing Maximum Packet Size.
/// The size check happens immediately after Remaining Length is decoded, before the
/// full packet body needs to be present in memory.
pub fn decode_packet_limited<'a>(
    mut raw_buf: &'a [u8],
    maximum_packet_size: usize,
) -> Result<(Packet<'a>, usize), CodecError> {
    let original_len = raw_buf.len();

    // 1. Read Fixed Header Byte
    let fixed_header_byte = read_u8(&mut raw_buf)?;
    let packet_type = fixed_header_byte >> 4;
    let flags = fixed_header_byte & 0x0F;

    // 2. Read Remaining Length
    let (remaining_len, read) = decode_varint(raw_buf)?;
    let total_packet_size = 1 + read + remaining_len as usize;
    if total_packet_size > maximum_packet_size {
        return Err(CodecError::PacketTooLarge);
    }
    raw_buf = &raw_buf[read..];

    if raw_buf.len() < remaining_len as usize {
        return Err(CodecError::Incomplete);
    }

    // Restrict our slice to the payload length of this packet
    let mut payload = &raw_buf[..remaining_len as usize];
    let total_read = original_len - raw_buf.len() + remaining_len as usize;

    match packet_type {
        1 => {
            // CONNECT
            if flags != 0 {
                return Err(CodecError::MalformedPacket);
            }
            let protocol_name = read_str(&mut payload)?;
            if protocol_name != "MQTT" {
                return Err(CodecError::MalformedPacket);
            }
            let protocol_level = read_u8(&mut payload)?;
            if protocol_level != 5 {
                return Err(CodecError::UnsupportedProtocolVersion(protocol_level));
            }
            let connect_flags = read_u8(&mut payload)?;
            let keep_alive = read_u16(&mut payload)?;

            if connect_flags & 0x01 != 0 {
                return Err(CodecError::MalformedPacket);
            }
            let clean_start = (connect_flags & 0x02) != 0;
            let has_will = (connect_flags & 0x04) != 0;
            let will_qos = (connect_flags & 0x18) >> 3;
            let will_retain = (connect_flags & 0x20) != 0;
            let has_username = (connect_flags & 0x80) != 0;
            let has_password = (connect_flags & 0x40) != 0;
            if (!has_will && (will_qos != 0 || will_retain)) || will_qos == 3 {
                return Err(CodecError::MalformedPacket);
            }

            let properties = ConnectProperties::decode(&mut payload)?;

            let client_id = read_str(&mut payload)?;

            let will = if has_will {
                let will_properties = WillProperties::decode(&mut payload)?;
                let will_topic = read_str(&mut payload)?;
                let will_payload = read_bytes(&mut payload)?;
                Some(Will {
                    qos: will_qos,
                    retain: will_retain,
                    topic: will_topic,
                    payload: will_payload,
                    properties: will_properties,
                })
            } else {
                None
            };

            let mut username = None;
            if has_username {
                username = Some(read_str(&mut payload)?);
            }

            let mut password = None;
            if has_password {
                password = Some(read_bytes(&mut payload)?);
            }

            Ok((
                Packet::Connect(Connect {
                    client_id,
                    keep_alive,
                    clean_start,
                    username,
                    password,
                    will,
                    properties,
                }),
                total_read,
            ))
        }
        2 => {
            // CONNACK
            let connack_flags = read_u8(&mut payload)?;
            let session_present = (connack_flags & 0x01) != 0;
            let reason_code = read_u8(&mut payload)?;
            let properties = ConnAckProperties::decode(&mut payload)?;
            Ok((
                Packet::ConnAck(ConnAck {
                    session_present,
                    reason_code,
                    properties,
                }),
                total_read,
            ))
        }
        3 => {
            // PUBLISH
            let dup = (flags & 0x08) != 0;
            let qos = (flags & 0x06) >> 1;
            let retain = (flags & 0x01) != 0;

            if qos > 2 {
                return Err(CodecError::MalformedPacket);
            }

            // PUBLISH topics are validated for MQTT-specific forbidden code points
            // in the session hot path together with wildcard validation, avoiding a
            // second Unicode walk for the overwhelmingly common ASCII case.
            let topic = read_utf8_str_structural(&mut payload)?;
            let mut packet_id = None;
            if qos > 0 {
                let pid = read_u16(&mut payload)?;
                if pid == 0 {
                    return Err(CodecError::ProtocolError);
                }
                packet_id = Some(pid);
            }

            let properties = PublishProperties::decode(&mut payload)?;
            let payload_bytes = payload; // Remaining bytes are payload

            Ok((
                Packet::Publish(Publish {
                    dup,
                    qos,
                    retain,
                    topic,
                    packet_id,
                    properties,
                    payload: payload_bytes,
                }),
                total_read,
            ))
        }
        4 | 5 | 6 | 7 => {
            let expected_flags = if packet_type == 6 { 0x02 } else { 0x00 };
            if flags != expected_flags {
                return Err(CodecError::MalformedPacket);
            }
            let packet_id = read_u16(&mut payload)?;
            let mut reason_code = 0;
            let mut properties = PubAckProperties::default();
            if !payload.is_empty() {
                reason_code = read_u8(&mut payload)?;
                if !payload.is_empty() {
                    properties = PubAckProperties::decode(&mut payload)?;
                }
            }
            let ack = PubAck {
                packet_id,
                reason_code,
                properties,
            };
            let packet = match packet_type {
                4 => Packet::PubAck(ack),
                5 => Packet::PubRec(ack),
                6 => Packet::PubRel(ack),
                7 => Packet::PubComp(ack),
                _ => unreachable!(),
            };
            Ok((packet, total_read))
        }
        8 => {
            // SUBSCRIBE
            if flags != 0x02 {
                // Must be 0010
                return Err(CodecError::MalformedPacket);
            }
            let packet_id = read_u16(&mut payload)?;
            if packet_id == 0 {
                return Err(CodecError::ProtocolError);
            }
            let properties = SubscribeProperties::decode(&mut payload)?;

            let mut subscriptions = Vec::new();
            while !payload.is_empty() {
                let topic_filter = read_str(&mut payload)?;
                let options = read_u8(&mut payload)?;
                subscriptions.push(Subscription {
                    topic_filter,
                    options,
                });
            }

            if subscriptions.is_empty() {
                return Err(CodecError::MalformedPacket);
            }

            Ok((
                Packet::Subscribe(Subscribe {
                    packet_id,
                    properties,
                    subscriptions,
                }),
                total_read,
            ))
        }
        9 => {
            // SUBACK
            let packet_id = read_u16(&mut payload)?;
            let properties = SubAckProperties::decode(&mut payload)?;
            let mut reason_codes = Vec::new();
            while !payload.is_empty() {
                reason_codes.push(read_u8(&mut payload)?);
            }
            Ok((
                Packet::SubAck(SubAck {
                    packet_id,
                    properties,
                    reason_codes,
                }),
                total_read,
            ))
        }
        10 => {
            // UNSUBSCRIBE
            if flags != 0x02 {
                return Err(CodecError::MalformedPacket);
            }
            let packet_id = read_u16(&mut payload)?;
            if packet_id == 0 {
                return Err(CodecError::ProtocolError);
            }
            let properties = UnsubscribeProperties::decode(&mut payload)?;
            let mut topic_filters = Vec::new();
            while !payload.is_empty() {
                topic_filters.push(read_str(&mut payload)?);
            }
            if topic_filters.is_empty() {
                return Err(CodecError::MalformedPacket);
            }
            Ok((
                Packet::Unsubscribe(Unsubscribe {
                    packet_id,
                    properties,
                    topic_filters,
                }),
                total_read,
            ))
        }
        11 => {
            // UNSUBACK
            if flags != 0 {
                return Err(CodecError::MalformedPacket);
            }
            let packet_id = read_u16(&mut payload)?;
            if packet_id == 0 {
                return Err(CodecError::ProtocolError);
            }
            let properties = SubAckProperties::decode(&mut payload)?;
            let mut reason_codes = Vec::new();
            while !payload.is_empty() {
                reason_codes.push(read_u8(&mut payload)?);
            }
            Ok((
                Packet::UnsubAck(UnsubAck {
                    packet_id,
                    properties,
                    reason_codes,
                }),
                total_read,
            ))
        }
        12 => {
            // PINGREQ
            if flags != 0 || remaining_len != 0 {
                return Err(CodecError::MalformedPacket);
            }
            Ok((Packet::PingReq, total_read))
        }
        13 => {
            // PINGRESP
            if flags != 0 || remaining_len != 0 {
                return Err(CodecError::MalformedPacket);
            }
            Ok((Packet::PingResp, total_read))
        }
        14 => {
            // DISCONNECT
            let mut reason_code = 0; // Default: Normal disconnection
            let mut properties = DisconnectProperties::default();
            if !payload.is_empty() {
                reason_code = read_u8(&mut payload)?;
                if !payload.is_empty() {
                    properties = DisconnectProperties::decode(&mut payload)?;
                }
            }
            Ok((
                Packet::Disconnect(Disconnect {
                    reason_code,
                    properties,
                }),
                total_read,
            ))
        }
        _ => Err(CodecError::MalformedPacket),
    }
}

// Packet encoders

/// Allocation-efficient MQTT v5 QoS 0 PUBLISH encoder.
/// Computes Remaining Length up front and writes directly into the final buffer,
/// avoiding the generic encoder's intermediate payload allocation.
pub fn encode_publish_qos0(
    topic: &str,
    payload: &[u8],
    retain: bool,
    subscription_identifier: Option<u32>,
) -> Vec<u8> {
    let subscription_varint_len = subscription_identifier.map(varint_encoded_len).unwrap_or(0);
    let properties_len = subscription_identifier
        .map(|_| 1 + subscription_varint_len) // property id + value
        .unwrap_or(0);
    let properties_len_varint = varint_encoded_len(properties_len as u32);
    let remaining_len = 2 + topic.len() + properties_len_varint + properties_len + payload.len();

    let mut buf = Vec::with_capacity(1 + varint_encoded_len(remaining_len as u32) + remaining_len);
    buf.push((3 << 4) | u8::from(retain));
    encode_varint(remaining_len as u32, &mut buf);
    write_str(topic, &mut buf);
    encode_varint(properties_len as u32, &mut buf);
    if let Some(identifier) = subscription_identifier {
        buf.push(0x0B);
        encode_varint(identifier, &mut buf);
    }
    buf.extend_from_slice(payload);
    buf
}

#[inline]
fn varint_encoded_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 128 {
        value /= 128;
        len += 1;
    }
    len
}

pub fn encode_packet(packet: &Packet, buf: &mut Vec<u8>) {
    let mut payload = Vec::new();
    let mut fixed_header_byte: u8;

    match packet {
        Packet::Connect(pkt) => {
            fixed_header_byte = 1 << 4;
            write_str("MQTT", &mut payload);
            payload.push(5); // Protocol version v5

            let mut connect_flags: u8 = 0;
            if pkt.clean_start {
                connect_flags |= 0x02;
            }
            if let Some(will) = &pkt.will {
                connect_flags |= 0x04;
                connect_flags |= (will.qos & 0x03) << 3;
                if will.retain {
                    connect_flags |= 0x20;
                }
            }
            if pkt.username.is_some() {
                connect_flags |= 0x80;
            }
            if pkt.password.is_some() {
                connect_flags |= 0x40;
            }
            payload.push(connect_flags);
            write_u16(pkt.keep_alive, &mut payload);

            pkt.properties.encode(&mut payload);
            write_str(pkt.client_id, &mut payload);

            if let Some(will) = &pkt.will {
                will.properties.encode(&mut payload);
                write_str(will.topic, &mut payload);
                write_bytes(will.payload, &mut payload);
            }
            if let Some(user) = pkt.username {
                write_str(user, &mut payload);
            }
            if let Some(pass) = pkt.password {
                write_bytes(pass, &mut payload);
            }
        }
        Packet::ConnAck(pkt) => {
            fixed_header_byte = 2 << 4;
            let flags: u8 = if pkt.session_present { 1 } else { 0 };
            payload.push(flags);
            payload.push(pkt.reason_code);
            pkt.properties.encode(&mut payload);
        }
        Packet::Publish(pkt) => {
            fixed_header_byte = 3 << 4;
            if pkt.dup {
                fixed_header_byte |= 0x08;
            }
            fixed_header_byte |= (pkt.qos & 0x03) << 1;
            if pkt.retain {
                fixed_header_byte |= 0x01;
            }

            write_str(pkt.topic, &mut payload);
            if let Some(pid) = pkt.packet_id {
                write_u16(pid, &mut payload);
            }
            pkt.properties.encode(&mut payload);
            payload.extend_from_slice(pkt.payload);
        }
        Packet::PubAck(pkt) | Packet::PubRec(pkt) | Packet::PubComp(pkt) => {
            fixed_header_byte = match packet {
                Packet::PubAck(_) => 4 << 4,
                Packet::PubRec(_) => 5 << 4,
                Packet::PubComp(_) => 7 << 4,
                _ => unreachable!(),
            };
            write_u16(pkt.packet_id, &mut payload);
            let has_props = pkt.properties.reason_string.is_some()
                || !pkt.properties.user_properties.is_empty();
            if has_props || pkt.reason_code != 0 {
                payload.push(pkt.reason_code);
                pkt.properties.encode(&mut payload);
            }
        }
        Packet::PubRel(pkt) => {
            fixed_header_byte = (6 << 4) | 0x02;
            write_u16(pkt.packet_id, &mut payload);
            let has_props = pkt.properties.reason_string.is_some()
                || !pkt.properties.user_properties.is_empty();
            if has_props || pkt.reason_code != 0 {
                payload.push(pkt.reason_code);
                pkt.properties.encode(&mut payload);
            }
        }
        Packet::Subscribe(pkt) => {
            fixed_header_byte = (8 << 4) | 0x02; // SUBSCRIBE flags must be 0010
            write_u16(pkt.packet_id, &mut payload);
            pkt.properties.encode(&mut payload);
            for sub in &pkt.subscriptions {
                write_str(sub.topic_filter, &mut payload);
                payload.push(sub.options);
            }
        }
        Packet::SubAck(pkt) => {
            fixed_header_byte = 9 << 4;
            write_u16(pkt.packet_id, &mut payload);
            pkt.properties.encode(&mut payload);
            payload.extend_from_slice(&pkt.reason_codes);
        }
        Packet::Unsubscribe(pkt) => {
            fixed_header_byte = (10 << 4) | 0x02;
            write_u16(pkt.packet_id, &mut payload);
            pkt.properties.encode(&mut payload);
            for filter in &pkt.topic_filters {
                write_str(filter, &mut payload);
            }
        }
        Packet::UnsubAck(pkt) => {
            fixed_header_byte = 11 << 4;
            write_u16(pkt.packet_id, &mut payload);
            pkt.properties.encode(&mut payload);
            payload.extend_from_slice(&pkt.reason_codes);
        }
        Packet::PingReq => {
            fixed_header_byte = 12 << 4;
        }
        Packet::PingResp => {
            fixed_header_byte = 13 << 4;
        }
        Packet::Disconnect(pkt) => {
            fixed_header_byte = 14 << 4;

            let has_props = pkt.properties.session_expiry_interval.is_some()
                || pkt.properties.reason_string.is_some()
                || pkt.properties.server_reference.is_some()
                || !pkt.properties.user_properties.is_empty();

            if has_props || pkt.reason_code != 0 {
                payload.push(pkt.reason_code);
                pkt.properties.encode(&mut payload);
            }
        }
    }

    buf.push(fixed_header_byte);
    encode_varint(payload.len() as u32, buf);
    buf.extend_from_slice(&payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_packet_parsing() {
        let connect_pkt = Packet::Connect(Connect {
            client_id: "pipistrelle_client",
            keep_alive: 60,
            clean_start: true,
            username: Some("admin"),
            password: Some(b"secret123"),
            will: None,
            properties: ConnectProperties {
                session_expiry_interval: Some(3600),
                receive_maximum: Some(100),
                max_packet_size: Some(65536),
                user_properties: vec![("env", "prod"), ("region", "eu")],
                ..Default::default()
            },
        });

        let mut buf = Vec::new();
        encode_packet(&connect_pkt, &mut buf);

        let (decoded, bytes_read) = decode_packet(&buf).unwrap();
        assert_eq!(bytes_read, buf.len());
        assert_eq!(decoded, connect_pkt);
    }

    #[test]
    fn test_connect_with_will_roundtrip() {
        let packet = Packet::Connect(Connect {
            client_id: "will-client",
            keep_alive: 30,
            clean_start: false,
            username: Some("admin"),
            password: Some(b"admin123"),
            will: Some(Will {
                qos: 2,
                retain: true,
                topic: "status/device",
                payload: b"offline",
                properties: WillProperties {
                    will_delay_interval: Some(5),
                    content_type: Some("text/plain"),
                    ..Default::default()
                },
            }),
            properties: ConnectProperties {
                session_expiry_interval: Some(30),
                ..Default::default()
            },
        });
        let mut buf = Vec::new();
        encode_packet(&packet, &mut buf);
        let (decoded, used) = decode_packet(&buf).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(decoded, packet);
    }

    #[test]
    fn test_qos2_ack_roundtrip_and_pubrel_flags() {
        for packet in [
            Packet::PubRec(PubAck {
                packet_id: 7,
                reason_code: 0,
                properties: Default::default(),
            }),
            Packet::PubRel(PubAck {
                packet_id: 7,
                reason_code: 0,
                properties: Default::default(),
            }),
            Packet::PubComp(PubAck {
                packet_id: 7,
                reason_code: 0,
                properties: Default::default(),
            }),
        ] {
            let mut buf = Vec::new();
            encode_packet(&packet, &mut buf);
            if matches!(packet, Packet::PubRel(_)) {
                assert_eq!(buf[0], 0x62);
            }
            let (decoded, used) = decode_packet(&buf).unwrap();
            assert_eq!(used, buf.len());
            assert_eq!(decoded, packet);
        }
    }

    #[test]
    fn test_connack_packet() {
        let connack_pkt = Packet::ConnAck(ConnAck {
            session_present: false,
            reason_code: 0,
            properties: ConnAckProperties {
                assigned_client_identifier: Some("generated_id_123"),
                topic_alias_maximum: Some(10),
                ..Default::default()
            },
        });

        let mut buf = Vec::new();
        encode_packet(&connack_pkt, &mut buf);

        let (decoded, bytes_read) = decode_packet(&buf).unwrap();
        assert_eq!(bytes_read, buf.len());
        assert_eq!(decoded, connack_pkt);
    }

    #[test]
    fn test_publish_packet() {
        let publish_pkt = Packet::Publish(Publish {
            dup: false,
            qos: 1,
            retain: false,
            topic: "sensor/temp/orange_pi",
            packet_id: Some(42),
            properties: PublishProperties {
                content_type: Some("application/json"),
                user_properties: vec![("device", "opi_zero3")],
                ..Default::default()
            },
            payload: b"{\"temperature\": 24.5}",
        });

        let mut buf = Vec::new();
        encode_packet(&publish_pkt, &mut buf);

        let (decoded, bytes_read) = decode_packet(&buf).unwrap();
        assert_eq!(bytes_read, buf.len());
        assert_eq!(decoded, publish_pkt);
    }

    #[test]
    fn test_direct_qos0_encoder_matches_generic_encoder() {
        for subscription_identifier in [None, Some(7)] {
            let publish_pkt = Packet::Publish(Publish {
                dup: false,
                qos: 0,
                retain: false,
                topic: "bench/native/1",
                packet_id: None,
                properties: PublishProperties {
                    subscription_identifiers: subscription_identifier.into_iter().collect(),
                    ..Default::default()
                },
                payload: b"payload",
            });
            let mut generic = Vec::new();
            encode_packet(&publish_pkt, &mut generic);
            let direct =
                encode_publish_qos0("bench/native/1", b"payload", false, subscription_identifier);
            assert_eq!(direct, generic);
        }
    }

    #[test]
    fn test_subscribe_packet() {
        let subscribe_pkt = Packet::Subscribe(Subscribe {
            packet_id: 101,
            properties: SubscribeProperties {
                subscription_identifier: Some(5),
                ..Default::default()
            },
            subscriptions: vec![
                Subscription {
                    topic_filter: "sensor/+/cpu",
                    options: 1, // QoS 1
                },
                Subscription {
                    topic_filter: "alerts/#",
                    options: 2, // QoS 2
                },
            ],
        });

        let mut buf = Vec::new();
        encode_packet(&subscribe_pkt, &mut buf);

        let (decoded, bytes_read) = decode_packet(&buf).unwrap();
        assert_eq!(bytes_read, buf.len());
        assert_eq!(decoded, subscribe_pkt);
    }

    #[test]
    fn test_ping_packets() {
        let mut buf = Vec::new();
        encode_packet(&Packet::PingReq, &mut buf);
        let (decoded_req, read_req) = decode_packet(&buf).unwrap();
        assert_eq!(read_req, buf.len());
        assert_eq!(decoded_req, Packet::PingReq);

        buf.clear();
        encode_packet(&Packet::PingResp, &mut buf);
        let (decoded_resp, read_resp) = decode_packet(&buf).unwrap();
        assert_eq!(read_resp, buf.len());
        assert_eq!(decoded_resp, Packet::PingResp);
    }

    #[test]
    fn unsubscribe_and_unsuback_roundtrip() {
        let unsubscribe = Packet::Unsubscribe(Unsubscribe {
            packet_id: 91,
            properties: UnsubscribeProperties {
                user_properties: vec![("source", "test")],
            },
            topic_filters: vec!["sensor/+", "alerts/#"],
        });
        let mut buf = Vec::new();
        encode_packet(&unsubscribe, &mut buf);
        assert_eq!(buf[0], 0xA2);
        let (decoded, used) = decode_packet(&buf).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(decoded, unsubscribe);

        let unsuback = Packet::UnsubAck(UnsubAck {
            packet_id: 91,
            properties: SubAckProperties::default(),
            reason_codes: vec![0x00, 0x11],
        });
        buf.clear();
        encode_packet(&unsuback, &mut buf);
        assert_eq!(buf[0], 0xB0);
        let (decoded, used) = decode_packet(&buf).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(decoded, unsuback);
    }

    #[test]
    fn connect_flow_limits_reject_zero_and_duplicates() {
        let zero_receive = Packet::Connect(Connect {
            client_id: "flow-zero",
            keep_alive: 60,
            clean_start: true,
            username: None,
            password: None,
            will: None,
            properties: ConnectProperties {
                receive_maximum: Some(0),
                ..Default::default()
            },
        });
        let mut buf = Vec::new();
        encode_packet(&zero_receive, &mut buf);
        assert_eq!(decode_packet(&buf), Err(CodecError::ProtocolError));

        let zero_packet = Packet::Connect(Connect {
            client_id: "packet-zero",
            keep_alive: 60,
            clean_start: true,
            username: None,
            password: None,
            will: None,
            properties: ConnectProperties {
                max_packet_size: Some(0),
                ..Default::default()
            },
        });
        buf.clear();
        encode_packet(&zero_packet, &mut buf);
        assert_eq!(decode_packet(&buf), Err(CodecError::ProtocolError));

        // CONNECT property length=6: two Receive Maximum entries.
        let mut body = Vec::new();
        write_str("MQTT", &mut body);
        body.extend_from_slice(&[5, 0x02, 0, 60, 6, 0x21, 0, 1, 0x21, 0, 2]);
        write_str("dup-flow", &mut body);
        let mut raw = vec![0x10];
        encode_varint(body.len() as u32, &mut raw);
        raw.extend_from_slice(&body);
        assert_eq!(decode_packet(&raw), Err(CodecError::ProtocolError));
    }

    #[test]
    fn mqtt_utf8_and_varint_are_strict() {
        let raw = [0, 3, b'a', 0, b'b'];
        let mut slice = raw.as_slice();
        assert_eq!(read_str(&mut slice), Err(CodecError::MalformedPacket));

        let raw = [0, 3, 0xEF, 0xB7, 0x90]; // U+FDD0 noncharacter
        let mut slice = raw.as_slice();
        assert_eq!(read_str(&mut slice), Err(CodecError::MalformedPacket));

        assert_eq!(decode_varint(&[0x80, 0x00]), Err(CodecError::InvalidVarint));
        assert_eq!(decode_varint(&[0x81, 0x00]), Err(CodecError::InvalidVarint));
        assert_eq!(decode_varint(&[0x80, 0x01]), Ok((128, 2)));
    }

    #[test]
    fn packet_total_len_works_from_fixed_header_prefix() {
        assert_eq!(packet_total_len(&[]), Ok(None));
        assert_eq!(packet_total_len(&[0x30]), Ok(None));
        assert_eq!(packet_total_len(&[0x30, 0x7f]), Ok(Some(129)));
        assert_eq!(packet_total_len(&[0x30, 0x80, 0x01]), Ok(Some(131)));
        assert_eq!(
            packet_total_len(&[0x30, 0x80, 0x00]),
            Err(CodecError::InvalidVarint)
        );
    }

    #[test]
    fn test_disconnect_packet() {
        let disconnect_pkt = Packet::Disconnect(Disconnect {
            reason_code: 4, // Disconnect with Will Message
            properties: DisconnectProperties {
                reason_string: Some("shutting down node"),
                ..Default::default()
            },
        });

        let mut buf = Vec::new();
        encode_packet(&disconnect_pkt, &mut buf);

        let (decoded, bytes_read) = decode_packet(&buf).unwrap();
        assert_eq!(bytes_read, buf.len());
        assert_eq!(decoded, disconnect_pkt);
    }
}
