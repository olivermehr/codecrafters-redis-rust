#[derive(Debug)]
pub enum RespError {
    IncorrectParams,
    InvalidSyntax,
    IncompleteTcpStream,
    TcpStreamDropped,
    Empty,
}
