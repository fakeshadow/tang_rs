use std::time::Instant;

pub struct Conn<C> {
    conn: C,
    marker: usize,
    birth: Instant,
}

impl<C> Conn<C> {
    pub(crate) fn new(conn: C, marker: usize) -> Self {
        Self {
            conn,
            marker,
            birth: Instant::now(),
        }
    }

    #[inline(always)]
    pub(crate) fn marker(&self) -> usize {
        self.marker
    }

    #[inline(always)]
    pub(crate) fn conn_ref(&self) -> &C {
        &self.conn
    }

    #[inline(always)]
    pub(crate) fn conn_ref_mut(&mut self) -> &mut C {
        &mut self.conn
    }

    #[inline(always)]
    pub(crate) fn into_conn(self) -> C {
        self.conn
    }
}

pub struct IdleConn<C> {
    conn: Conn<C>,
    pub(crate) idle_start: Instant,
}

impl<C> IdleConn<C> {
    pub(crate) fn new(conn: C, marker: usize) -> Self {
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

    #[inline(always)]
    pub(crate) fn marker(&self) -> usize {
        self.conn.marker()
    }

    #[inline(always)]
    pub(crate) fn birth(&self) -> Instant {
        self.conn.birth
    }

    #[inline(always)]
    pub(crate) fn idle_start(&self) -> Instant {
        self.idle_start
    }
}

impl<C> From<Conn<C>> for IdleConn<C> {
    fn from(conn: Conn<C>) -> Self {
        let now = Instant::now();
        Self {
            conn,
            idle_start: now,
        }
    }
}

impl<C> From<IdleConn<C>> for Conn<C> {
    fn from(conn: IdleConn<C>) -> Self {
        Conn {
            conn: conn.conn.conn,
            birth: conn.conn.birth,
            marker: conn.conn.marker,
        }
    }
}
