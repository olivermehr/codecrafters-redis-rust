use std::rc::Rc;

use crate::resp::error::RespError;

#[derive(Debug, Clone)]
pub enum RespNode {
    Array(Vec<RespNode>),
    SimpleString(String),
    BulkString(String),
}
#[derive(Debug)]
pub struct RespParser {
    text: Vec<char>,
    pos: usize,
    len: usize,
    current_char: Option<char>,
}

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

    fn integer(&mut self) -> Result<i32, RespError> {
        let mut num = String::from(self.current_char.unwrap());
        self.advance();
        while let Some(char) = self.current_char
            && char.is_numeric()
        {
            num.push(char);
            self.advance();
        }
        num.parse().map_err(|e| RespError::InvalidSyntax)
    }

    fn bulk_string(&mut self, len: i32) -> Result<String, RespError> {
        let mut s = String::new();
        for _ in 0..len {
            match self.current_char {
                Some(char) => s.push(char),
                None => Err(RespError::InvalidSyntax)?,
            }
            self.advance();
        }
        Ok(s.to_ascii_lowercase())
    }
    pub fn create_tree(&mut self) -> Result<Vec<RespNode>, RespError> {
        let mut v = Vec::new();
        if self.pos >= self.len {
            return Ok(v);
        }
        self.current_char = Some(self.text[self.pos]);
        while self.pos < self.len {
            let node = self.get_next_node()?;
            v.push(node);
        }
        Ok(v)
    }

    fn crlf(&mut self) -> Result<(), RespError> {
        let tokens = ['\r', '\n'];
        for i in tokens {
            if let Some(char) = self.current_char
                && char == i
            {
                self.advance();
            } else {
                Err(RespError::InvalidSyntax)?
            }
        }
        Ok(())
    }

    fn get_next_node(&mut self) -> Result<RespNode, RespError> {
        let char = self.current_char.unwrap();
        if char == '*' {
            self.advance();
            let l = self.integer()?;
            let mut children = Vec::new();
            self.crlf()?;
            for _ in 0..l {
                children.push(self.get_next_node()?);
                self.crlf()?;
            }
            if children.len() as i32 != l {
                Err(RespError::InvalidSyntax)?
            }
            Ok(RespNode::Array(children))
        } else if char == '$' {
            self.advance();
            let l = self.integer()?;
            self.crlf()?;
            let s = self.bulk_string(l)?;
            Ok(RespNode::BulkString(s))
        } else {
            panic!("unsupported token")
        }
    }
}
