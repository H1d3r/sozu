pub fn http_response<S: Into<String>>(content: S) -> String {
    let content = content.into();
    let status_line = "HTTP/1.1 200 OK";
    let length = content.len();
    format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{content}")
}

pub fn http_request<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
    method: S1,
    uri: S2,
    content: S3,
) -> String {
    format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n{}",
        method.into(),
        uri.into(),
        content.into()
    )
}
