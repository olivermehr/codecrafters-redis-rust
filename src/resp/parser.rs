use std::rc::Rc;

use crate::resp::error::RespError;

#[derive(Debug, Clone)]
pub enum RespNode {
    Array(Vec<RespNode>),
    SimpleString(String),
    BulkString(String),
    Integer(isize),
}
#[derive(Debug)]
pub struct RespParser {
    pub text: Vec<char>,
    pos: usize,
    len: usize,
    current_char: Option<char>,
}

static UNREPLICATED_FNS: &[&str] = &["ping", "echo", "replconf", "psync"];

impl RespParser {
    pub fn new(t: Vec<char>) -> Self {
        let len = t.len();
        Self {
            text: t,
            pos: 0,
            len,
            current_char: None,
        }
    }
    fn advance(&mut self) {
        self.pos += 1;
        if self.pos < self.len {
            self.current_char = Some(self.text[self.pos]);
        } else {
            self.current_char = None
        }
    }

    fn peek(&self, offset: usize) -> Option<char> {
        if self.pos + offset < self.len {
            return Some(self.text[self.pos + offset]);
        }
        None
    }

    fn simple_string(&mut self) -> Result<String, RespError> {
        let mut s = String::new();
        while let Some(char) = self.current_char {
            if char == '\r' {
                return Ok(s);
            } else {
                s.push(char);
                self.advance();
            }
        }
        Err(RespError::IncompleteTcpStream)
    }

    fn integer(&mut self) -> Result<i32, RespError> {
        let mut num = String::new();
        while let Some(char) = self.current_char {
            if char.is_numeric() {
                num.push(char);
                self.advance();
            } else {
                return num.parse().map_err(|e| RespError::InvalidSyntax);
            }
        }
        Err(RespError::IncompleteTcpStream)
    }

    fn bulk_string(&mut self, len: i32) -> Result<String, RespError> {
        let mut s = String::new();
        for _ in 0..len {
            match self.current_char {
                Some(char) => s.push(char),
                None => Err(RespError::IncompleteTcpStream)?,
            }
            self.advance();
        }
        Ok(s.to_ascii_lowercase())
    }

    pub fn create_tree(&mut self) -> (usize, Vec<(RespNode, String)>) {
        println!("{:?}",self.text);
        let mut v = Vec::new();
        let mut bytes_used = 0;
        if self.pos >= self.len {
            return (bytes_used, v);
        }
        self.current_char = Some(self.text[self.pos]);
        while self.pos < self.len {
            let node = self.get_next_node();
            match node {
                Ok(n) => {
                    let processed_string = self.text[..self.pos].iter().collect::<String>();
                    let processed_string_len = processed_string.len();

                    let replicated_string = if let RespNode::Array(v) = &n
                        && let Some(RespNode::BulkString(s)) = v.first()
                        && !UNREPLICATED_FNS.contains(&s.as_str())
                    {
                        processed_string
                    } else {
                        String::new()
                    };
                    let processed_bytes = processed_string_len;
                    v.push((n, replicated_string));
                    bytes_used = processed_bytes;
                }
                Err(e) => {}
            }
        }
        (bytes_used, v)
    }

    fn crlf(&mut self) -> Result<(), RespError> {
        let tokens = ['\r', '\n'];
        for i in tokens {
            if let Some(char) = self.current_char
                && char == i
            {
                self.advance();
            } else {
                Err(RespError::IncompleteTcpStream)?
            }
        }
        Ok(())
    }

    fn get_next_node(&mut self) -> Result<RespNode, RespError> {
        let char = self.current_char.ok_or(RespError::IncompleteTcpStream)?;
        if char == '*' {
            self.advance();
            let l = self.integer()?;
            let mut children = Vec::new();
            self.crlf()?;
            for _ in 0..l {
                children.push(self.get_next_node()?);
                self.crlf()?;
            }
            Ok(RespNode::Array(children))
        } else if char == '$' {
            self.advance();
            let l = self.integer()?;
            self.crlf()?;
            let s = self.bulk_string(l)?;
            Ok(RespNode::BulkString(s))
        } else if char == ':' {
            self.advance();
            let int = self.integer()?;
            Ok(RespNode::Integer(int as isize))
        } else if char == '+' {
            self.advance();
            let s = self.simple_string()?;
            self.crlf()?;
            Ok(RespNode::SimpleString(s))
        } else {
            panic!("unsupported token {:?}", self.current_char)
        }
    }
}
