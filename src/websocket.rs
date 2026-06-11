use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use futures_util::{Stream, Sink};
use pin_project::pin_project;

#[pin_project]
pub struct WebSocketStreamAdapter<S> {
    #[pin]
    pub ws: WebSocketStream<S>,
    read_buffer: Vec<u8>,
    read_cursor: usize,
    write_buffer: Vec<u8>,
}

impl<S> WebSocketStreamAdapter<S> {
    pub fn new(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            read_buffer: Vec::new(),
            read_cursor: 0,
            write_buffer: Vec::new(),
        }
    }
}

impl<S> AsyncRead for WebSocketStreamAdapter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();
        
        loop {
            // If we have data in the buffer, read it
            if *this.read_cursor < this.read_buffer.len() {
                let available = this.read_buffer.len() - *this.read_cursor;
                let to_copy = std::cmp::min(available, buf.remaining());
                buf.put_slice(&this.read_buffer[*this.read_cursor..*this.read_cursor + to_copy]);
                *this.read_cursor += to_copy;
                return Poll::Ready(Ok(()));
            }
            
            // Otherwise, get next frame from WebSocket
            match this.ws.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    match msg {
                        Message::Binary(bin) => {
                            if bin.is_empty() {
                                continue;
                            }
                            *this.read_buffer = bin;
                            *this.read_cursor = 0;
                        }
                        Message::Text(txt) => {
                            if txt.is_empty() {
                                continue;
                            }
                            *this.read_buffer = txt.into_bytes();
                            *this.read_cursor = 0;
                        }
                        Message::Close(_) => {
                            return Poll::Ready(Ok(())); // EOF
                        }
                        Message::Ping(_) | Message::Pong(_) => {
                            continue;
                        }
                        _ => continue,
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Ok(())); // EOF
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<S> AsyncWrite for WebSocketStreamAdapter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        this.write_buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.as_mut().project();
        if this.write_buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        match this.ws.as_mut().poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = Message::Binary(std::mem::take(this.write_buffer));
                match this.ws.as_mut().start_send(msg) {
                    Ok(()) => {
                        match this.ws.as_mut().poll_flush(cx) {
                            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                            Poll::Pending => Poll::Pending,
                        }
                    }
                    Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(cx))?;
        
        let this = self.project();
        match this.ws.poll_close(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            Poll::Pending => Poll::Pending,
        }
    }
}
