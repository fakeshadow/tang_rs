use std::time::Instant;

use crate::manager::Manager;

pub struct Conn<M: Manager> {
    conn: M::Connection,
    marker: usize,
    birth: Instant,
}

impl<M: Manager> Conn<M> {
    pub(crate) fn new(conn: M::Connection, marker: usize) -> Self {
        Self {
            conn,
            marker,
            birth: Instant::now(),
        }
    }

    pub(crate) fn marker(&self) -> usize {
        self.marker
    }

    pub(crate) fn conn(&mut self) -> &mut M::Connection {
        &mut self.conn
    }
}

pub struct IdleConn<M: Manager> {
    conn: Conn<M>,
    idle_start: Instant,
}

impl<M: Manager> IdleConn<M> {
    fn new(conn: M::Connection, marker: usize) -> Self {
        let now = Instant::now();
        IdleConn {
            conn: Conn {
                conn,
                marker,
                birth: now,
            },
            idle_start: now,
        }
    }

    pub(crate) fn marker(&self) -> usize {
        self.conn.marker()
    }

    pub(crate) fn conn(&mut self) -> &mut M::Connection {
        &mut self.conn.conn
    }
}

impl<M: Manager> From<Conn<M>> for IdleConn<M> {
    fn from(conn: Conn<M>) -> IdleConn<M> {
        let now = Instant::now();
        IdleConn {
            conn,
            idle_start: now,
        }
    }
}

impl<M: Manager> From<IdleConn<M>> for Conn<M> {
    fn from(conn: IdleConn<M>) -> Conn<M> {
        Conn {
            conn: conn.conn.conn,
            birth: conn.conn.birth,
            marker: conn.conn.marker,
        }
    }
}
