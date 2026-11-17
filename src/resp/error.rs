#[derive(Debug)]
pub enum RespError {
    IncorrectParams,
    InvalidSyntax,
    IncompleteTcpStream,
    Empty,
}
