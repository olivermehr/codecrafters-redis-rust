use std::rc::Rc;

use crate::resp::{
    error::RespError,
    parser::RespNode::{BulkString, RdbFile},
};

#[derive(Debug, Clone)]
pub enum RespNode {
    Array(Vec<RespNode>),
    SimpleString(String),
    BulkString(String),
    Integer(isize),
    RdbFile(Vec<u8>),
}
#[derive(Debug)]
pub struct RespParser<'a> {
    pub bytes: &'a [u8],
    pos: usize,
    len: usize,
    current_byte: Option<u8>,
}

static UNREPLICATED_FNS: &[&str] = &["ping", "echo", "replconf", "psync"];

impl<'a> RespParser<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        let len = b.len();
        Self {
            bytes: b,
            pos: 0,
            len,
            current_byte: None,
        }
    }
    fn advance(&mut self) {
        self.pos += 1;
        if self.pos < self.len {
            self.current_byte = Some(self.bytes[self.pos]);
        } else {
            self.current_byte = None
        }
    }

    fn peek(&self, offset: usize) -> Option<u8> {
        if self.pos + offset < self.len {
            return Some(self.bytes[self.pos + offset]);
        }
        None
    }

    fn simple_string(&mut self) -> Result<String, RespError> {
        let mut s = Vec::new();
        while let Some(byte) = self.current_byte {
            if byte == b'\r' {
                return Ok(String::from_utf8(s).unwrap());
            } else {
                s.push(byte);
                self.advance();
            }
        }
        Err(RespError::IncompleteTcpStream)
    }

    fn integer(&mut self) -> Result<i32, RespError> {
        let mut num = Vec::new();
        while let Some(byte) = self.current_byte {
            if byte.is_ascii_digit() {
                num.push(byte);
                self.advance();
            } else {
                let num = String::from_utf8(num).unwrap();
                return num.parse().map_err(|e| RespError::InvalidSyntax);
            }
        }
        Err(RespError::IncompleteTcpStream)
    }

    fn bulk_string(&mut self, len: i32) -> Result<String, RespError> {
        let mut s = Vec::new();
        for _ in 0..len {
            match self.current_byte {
                Some(byte) => s.push(byte),
                None => Err(RespError::IncompleteTcpStream)?,
            }
            self.advance();
        }
        Ok(String::from_utf8(s).unwrap().to_lowercase())
    }

    fn rdb_file(&mut self, len: i32) -> Result<Vec<u8>, RespError> {
        let mut v = Vec::new();
        for _ in 0..len {
            match self.current_byte {
                Some(byte) => v.push(byte),
                None => Err(RespError::IncompleteTcpStream)?,
            }
            self.advance();
        }
        Ok(v)
    }

    pub fn create_tree(
        &mut self,
        replica_handshake_mode: bool,
    ) -> (usize, Vec<(RespNode, String)>) {
        let mut v = Vec::new();
        let mut bytes_used = 0;
        if self.pos >= self.len {
            return (bytes_used, v);
        }
        self.current_byte = Some(self.bytes[self.pos]);
        while self.pos < self.len {
            let node = self.get_next_node(replica_handshake_mode);
            match node {
                Ok(n) => {
                    let replicated_string = if let RespNode::Array(v) = &n
                        && let Some(RespNode::BulkString(s)) = v.first()
                        && !UNREPLICATED_FNS.contains(&s.as_str())
                    {
                        let s = String::from_utf8(self.bytes[..self.pos].to_vec()).unwrap();
                        s
                    } else {
                        String::new()
                    };
                    v.push((n, replicated_string));
                    bytes_used = self.pos;
                }
                Err(e) => {}
            }
        }
        (bytes_used, v)
    }

    fn crlf(&mut self) -> Result<(), RespError> {
        let tokens = *b"\r\n";
        for i in tokens {
            if let Some(byte) = self.current_byte
                && byte == i
            {
                self.advance();
            } else {
                Err(RespError::IncompleteTcpStream)?
            }
        }
        Ok(())
    }

    fn get_next_node(&mut self, replica_handshake_mode: bool) -> Result<RespNode, RespError> {
        let byte = self.current_byte.ok_or(RespError::IncompleteTcpStream)?;
        if byte == b'*' {
            self.advance();
            let l = self.integer()?;
            let mut children = Vec::new();
            self.crlf()?;
            for _ in 0..l {
                children.push(self.get_next_node(replica_handshake_mode)?);
                self.crlf()?;
            }
            Ok(RespNode::Array(children))
        } else if byte == b'$' {
            self.advance();
            let l = self.integer()?;
            self.crlf()?;
            if replica_handshake_mode {
                let rdb_file = self.rdb_file(l)?;
                Ok(RdbFile(rdb_file))
            } else {
                let s = self.bulk_string(l)?;
                Ok(BulkString(s))
            }
        } else if byte == b':' {
            self.advance();
            let int = self.integer()?;
            Ok(RespNode::Integer(int as isize))
        } else if byte == b'+' {
            self.advance();
            let s = self.simple_string()?;
            self.crlf()?;
            Ok(RespNode::SimpleString(s))
        } else {
            panic!("unsupported token {:?}", self.current_byte)
        }
    }
}
