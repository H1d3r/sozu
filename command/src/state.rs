use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    iter::{repeat, FromIterator},
    net::SocketAddr,
};

use serde::de::{self, Visitor};

use crate::{
    certificate::calculate_fingerprint,
    proxy::{
        ActivateListener, AddCertificate, Backend, CertificateAndKey, CertificateFingerprint,
        Cluster, DeactivateListener, HttpFrontend, HttpListener, HttpsListener, ListenerType,
        PathRule, ProxyRequestOrder, QueryAnswerCluster, RemoveBackend, RemoveCertificate,
        RemoveListener, Route, TcpFrontend, TcpListener,
    },
};

pub type ClusterId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpProxy {
    address: SocketAddr,
    fronts: HashMap<ClusterId, Vec<HttpFrontend>>,
    backends: HashMap<ClusterId, Vec<Backend>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpsProxy {
    address: SocketAddr,
    certificates: HashMap<CertificateFingerprint, CertificateAndKey>,
    fronts: HashMap<ClusterId, Vec<HttpFrontend>>,
    backends: HashMap<ClusterId, Vec<Backend>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigState {
    pub clusters: BTreeMap<ClusterId, Cluster>,
    pub backends: BTreeMap<ClusterId, Vec<Backend>>,
    /// the bool indicates if it is active or not
    pub http_listeners: HashMap<SocketAddr, (HttpListener, bool)>,
    pub https_listeners: HashMap<SocketAddr, (HttpsListener, bool)>,
    pub tcp_listeners: HashMap<SocketAddr, (TcpListener, bool)>,
    /// indexed by (address, hostname, path)
    pub http_fronts: BTreeMap<RouteKey, HttpFrontend>,
    /// indexed by (address, hostname, path)
    pub https_fronts: BTreeMap<RouteKey, HttpFrontend>,
    pub tcp_fronts: HashMap<ClusterId, Vec<TcpFrontend>>,
    /// certificate and names
    pub certificates:
        HashMap<SocketAddr, HashMap<CertificateFingerprint, (CertificateAndKey, Vec<String>)>>,
    //ip, port
    pub http_addresses: Vec<SocketAddr>,
    pub https_addresses: Vec<SocketAddr>,
    //tcp:
}

impl ConfigState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_http_address(&mut self, address: SocketAddr) {
        self.http_addresses.push(address)
    }

    pub fn add_https_address(&mut self, address: SocketAddr) {
        self.https_addresses.push(address)
    }

    /// returns true if the order modified something
    pub fn handle_order(&mut self, order: &ProxyRequestOrder) -> bool {
        match order {
            &ProxyRequestOrder::AddCluster(ref cluster) => {
                let cluster = cluster.clone();
                self.clusters.insert(cluster.cluster_id.clone(), cluster);
                true
            }
            &ProxyRequestOrder::RemoveCluster { ref cluster_id } => {
                self.clusters.remove(cluster_id).is_some()
            }
            &ProxyRequestOrder::AddHttpListener(ref listener) => {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.http_listeners.entry(listener.address)
                {
                    e.insert((listener.clone(), false));
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::AddHttpsListener(ref listener) => {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.https_listeners.entry(listener.address)
                {
                    e.insert((listener.clone(), false));
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::AddTcpListener(ref listener) => {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.tcp_listeners.entry(listener.address)
                {
                    e.insert((listener.clone(), false));
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::RemoveListener(ref remove) => match remove.proxy {
                ListenerType::HTTP => self.http_listeners.remove(&remove.address).is_some(),
                ListenerType::HTTPS => self.https_listeners.remove(&remove.address).is_some(),
                ListenerType::TCP => self.tcp_listeners.remove(&remove.address).is_some(),
            },
            &ProxyRequestOrder::ActivateListener(ref activate) => match activate.proxy {
                ListenerType::HTTP => self
                    .http_listeners
                    .get_mut(&activate.address)
                    .map(|t| t.1 = true)
                    .is_some(),
                ListenerType::HTTPS => self
                    .https_listeners
                    .get_mut(&activate.address)
                    .map(|t| t.1 = true)
                    .is_some(),
                ListenerType::TCP => self
                    .tcp_listeners
                    .get_mut(&activate.address)
                    .map(|t| t.1 = true)
                    .is_some(),
            },
            &ProxyRequestOrder::DeactivateListener(ref deactivate) => match deactivate.proxy {
                ListenerType::HTTP => self
                    .http_listeners
                    .get_mut(&deactivate.address)
                    .map(|t| t.1 = false)
                    .is_some(),
                ListenerType::HTTPS => self
                    .https_listeners
                    .get_mut(&deactivate.address)
                    .map(|t| t.1 = false)
                    .is_some(),
                ListenerType::TCP => self
                    .tcp_listeners
                    .get_mut(&deactivate.address)
                    .map(|t| t.1 = false)
                    .is_some(),
            },
            &ProxyRequestOrder::AddHttpFrontend(ref front) => {
                if let std::collections::btree_map::Entry::Vacant(e) =
                    self.http_fronts.entry(RouteKey(
                        front.address,
                        front.hostname.to_string(),
                        front.path.clone(),
                        front.method.clone(),
                    ))
                {
                    e.insert(front.clone());
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::RemoveHttpFrontend(ref front) => self
                .http_fronts
                .remove(&RouteKey(
                    front.address,
                    front.hostname.to_string(),
                    front.path.clone(),
                    front.method.clone(),
                ))
                .is_some(),
            &ProxyRequestOrder::AddCertificate(ref add) => {
                let fingerprint =
                    match calculate_fingerprint(add.certificate.certificate.as_bytes()) {
                        Ok(f) => CertificateFingerprint(f),
                        Err(e) => {
                            error!(
                                "cannot obtain the certificate's fingerprint: {}",
                                e.to_string()
                            );
                            return false;
                        }
                    };

                self.certificates
                    .entry(add.address)
                    .or_insert_with(HashMap::new);
                if !self
                    .certificates
                    .get(&add.address)
                    .unwrap()
                    .contains_key(&fingerprint)
                {
                    self.certificates
                        .get_mut(&add.address)
                        .unwrap()
                        .insert(fingerprint, (add.certificate.clone(), add.names.clone()));
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::RemoveCertificate(ref remove) => self
                .certificates
                .get_mut(&remove.address)
                .and_then(|certs| certs.remove(&remove.fingerprint))
                .is_some(),
            &ProxyRequestOrder::ReplaceCertificate(ref replace) => {
                let changed = self
                    .certificates
                    .get_mut(&replace.address)
                    .and_then(|certs| certs.remove(&replace.old_fingerprint))
                    .is_some();

                let fingerprint =
                    match calculate_fingerprint(replace.new_certificate.certificate.as_bytes()) {
                        Ok(f) => CertificateFingerprint(f),
                        Err(e) => {
                            error!(
                                "cannot obtain the certificate's fingerprint: {}",
                                e.to_string()
                            );
                            return changed;
                        }
                    };

                if !self
                    .certificates
                    .get(&replace.address)
                    .unwrap()
                    .contains_key(&fingerprint)
                {
                    self.certificates.get_mut(&replace.address).map(|certs| {
                        certs.insert(
                            fingerprint.clone(),
                            (replace.new_certificate.clone(), replace.new_names.clone()),
                        )
                    });

                    true
                } else {
                    changed
                }
            }
            &ProxyRequestOrder::AddHttpsFrontend(ref front) => {
                if let std::collections::btree_map::Entry::Vacant(e) =
                    self.https_fronts.entry(RouteKey(
                        front.address,
                        front.hostname.to_string(),
                        front.path.clone(),
                        front.method.clone(),
                    ))
                {
                    e.insert(front.clone());
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::RemoveHttpsFrontend(ref front) => self
                .https_fronts
                .remove(&RouteKey(
                    front.address,
                    front.hostname.to_string(),
                    front.path.clone(),
                    front.method.clone(),
                ))
                .is_some(),
            &ProxyRequestOrder::AddTcpFrontend(ref front) => {
                let front_vec = self
                    .tcp_fronts
                    .entry(front.cluster_id.clone())
                    .or_insert_with(Vec::new);
                if !front_vec.contains(front) {
                    front_vec.push(front.clone());
                    true
                } else {
                    false
                }
            }
            &ProxyRequestOrder::RemoveTcpFrontend(ref front) => {
                if let Some(front_list) = self.tcp_fronts.get_mut(&front.cluster_id) {
                    let len = front_list.len();
                    front_list.retain(|el| el.address != front.address);
                    front_list.len() != len
                } else {
                    false
                }
            }
            &ProxyRequestOrder::AddBackend(ref backend) => {
                let backend_vec = self
                    .backends
                    .entry(backend.cluster_id.clone())
                    .or_insert_with(Vec::new);

                // we might be modifying the sticky id or load balancing parameters
                backend_vec
                    .retain(|b| b.backend_id != backend.backend_id || b.address != backend.address);
                backend_vec.push(backend.clone());
                backend_vec.sort();

                true
            }
            &ProxyRequestOrder::RemoveBackend(ref backend) => {
                if let Some(backend_list) = self.backends.get_mut(&backend.cluster_id) {
                    let len = backend_list.len();
                    backend_list.retain(|b| {
                        b.backend_id != backend.backend_id || b.address != backend.address
                    });
                    backend_list.sort();
                    backend_list.len() != len
                } else {
                    false
                }
            }
            // This is to avoid the error message
            &ProxyRequestOrder::Logging(_)
            | &ProxyRequestOrder::Status
            | &ProxyRequestOrder::Query(_)
            | &ProxyRequestOrder::SoftStop
            | &ProxyRequestOrder::HardStop => false,
            o => {
                error!("state cannot handle order message: {:#?}", o);
                false
            }
        }
    }

    pub fn generate_orders(&self) -> Vec<ProxyRequestOrder> {
        let mut v = Vec::new();

        for &(ref listener, active) in self.http_listeners.values() {
            v.push(ProxyRequestOrder::AddHttpListener(listener.clone()));
            if active {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: listener.address,
                    proxy: ListenerType::HTTP,
                    from_scm: false,
                }));
            }
        }

        for &(ref listener, active) in self.https_listeners.values() {
            v.push(ProxyRequestOrder::AddHttpsListener(listener.clone()));
            if active {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: listener.address,
                    proxy: ListenerType::HTTPS,
                    from_scm: false,
                }));
            }
        }

        for &(ref listener, active) in self.tcp_listeners.values() {
            v.push(ProxyRequestOrder::AddTcpListener(listener.clone()));
            if active {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: listener.address,
                    proxy: ListenerType::TCP,
                    from_scm: false,
                }));
            }
        }

        for cluster in self.clusters.values() {
            v.push(ProxyRequestOrder::AddCluster(cluster.clone()));
        }

        for front in self.http_fronts.values() {
            v.push(ProxyRequestOrder::AddHttpFrontend(front.clone()));
        }

        for (front, certs) in self.certificates.iter() {
            for &(ref certificate_and_key, ref names) in certs.values() {
                v.push(ProxyRequestOrder::AddCertificate(AddCertificate {
                    address: *front,
                    certificate: certificate_and_key.clone(),
                    names: names.clone(),
                    expired_at: None,
                }));
            }
        }

        for front in self.https_fronts.values() {
            v.push(ProxyRequestOrder::AddHttpsFrontend(front.clone()));
        }

        for front_list in self.tcp_fronts.values() {
            for front in front_list {
                v.push(ProxyRequestOrder::AddTcpFrontend(front.clone()));
            }
        }

        for backend_list in self.backends.values() {
            for backend in backend_list {
                v.push(ProxyRequestOrder::AddBackend(backend.clone()));
            }
        }

        v
    }

    pub fn generate_activate_orders(&self) -> Vec<ProxyRequestOrder> {
        let mut v = Vec::new();
        for front in self
            .http_listeners
            .iter()
            .filter(|(_, t)| t.1)
            .map(|(k, _)| k)
        {
            v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                address: *front,
                proxy: ListenerType::HTTP,
                from_scm: false,
            }));
        }

        for front in self
            .https_listeners
            .iter()
            .filter(|(_, t)| t.1)
            .map(|(k, _)| k)
        {
            v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                address: *front,
                proxy: ListenerType::HTTPS,
                from_scm: false,
            }));
        }
        for front in self
            .tcp_listeners
            .iter()
            .filter(|(_, t)| t.1)
            .map(|(k, _)| k)
        {
            v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                address: *front,
                proxy: ListenerType::TCP,
                from_scm: false,
            }));
        }

        v
    }

    pub fn diff(&self, other: &ConfigState) -> Vec<ProxyRequestOrder> {
        //pub tcp_listeners:   HashMap<SocketAddr, (TcpListener, bool)>,
        let my_tcp_listeners: HashSet<&SocketAddr> = self.tcp_listeners.keys().collect();
        let their_tcp_listeners: HashSet<&SocketAddr> = other.tcp_listeners.keys().collect();
        let removed_tcp_listeners = my_tcp_listeners.difference(&their_tcp_listeners);
        let added_tcp_listeners = their_tcp_listeners.difference(&my_tcp_listeners);

        let my_http_listeners: HashSet<&SocketAddr> = self.http_listeners.keys().collect();
        let their_http_listeners: HashSet<&SocketAddr> = other.http_listeners.keys().collect();
        let removed_http_listeners = my_http_listeners.difference(&their_http_listeners);
        let added_http_listeners = their_http_listeners.difference(&my_http_listeners);

        let my_https_listeners: HashSet<&SocketAddr> = self.https_listeners.keys().collect();
        let their_https_listeners: HashSet<&SocketAddr> = other.https_listeners.keys().collect();
        let removed_https_listeners = my_https_listeners.difference(&their_https_listeners);
        let added_https_listeners = their_https_listeners.difference(&my_https_listeners);

        let mut v = vec![];

        for address in removed_tcp_listeners {
            if self.tcp_listeners[address].1 {
                v.push(ProxyRequestOrder::DeactivateListener(DeactivateListener {
                    address: **address,
                    proxy: ListenerType::TCP,
                    to_scm: false,
                }));
            }

            v.push(ProxyRequestOrder::RemoveListener(RemoveListener {
                address: **address,
                proxy: ListenerType::TCP,
            }));
        }

        for address in added_tcp_listeners.clone() {
            v.push(ProxyRequestOrder::AddTcpListener(
                other.tcp_listeners[address].0.clone(),
            ));

            if other.tcp_listeners[address].1 {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: **address,
                    proxy: ListenerType::TCP,
                    from_scm: false,
                }));
            }
        }

        for address in removed_http_listeners {
            if self.http_listeners[address].1 {
                v.push(ProxyRequestOrder::DeactivateListener(DeactivateListener {
                    address: **address,
                    proxy: ListenerType::HTTP,
                    to_scm: false,
                }));
            }

            v.push(ProxyRequestOrder::RemoveListener(RemoveListener {
                address: **address,
                proxy: ListenerType::HTTP,
            }));
        }

        for address in added_http_listeners.clone() {
            v.push(ProxyRequestOrder::AddHttpListener(
                other.http_listeners[address].0.clone(),
            ));

            if other.http_listeners[address].1 {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: **address,
                    proxy: ListenerType::HTTP,
                    from_scm: false,
                }));
            }
        }

        for address in removed_https_listeners {
            if self.https_listeners[address].1 {
                v.push(ProxyRequestOrder::DeactivateListener(DeactivateListener {
                    address: **address,
                    proxy: ListenerType::HTTPS,
                    to_scm: false,
                }));
            }

            v.push(ProxyRequestOrder::RemoveListener(RemoveListener {
                address: **address,
                proxy: ListenerType::HTTPS,
            }));
        }

        for address in added_https_listeners.clone() {
            v.push(ProxyRequestOrder::AddHttpsListener(
                other.https_listeners[address].0.clone(),
            ));

            if other.https_listeners[address].1 {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: **address,
                    proxy: ListenerType::HTTPS,
                    from_scm: false,
                }));
            }
        }

        for addr in my_tcp_listeners.intersection(&their_tcp_listeners) {
            let (my_listener, my_active) = &self.tcp_listeners[addr];
            let (their_listener, their_active) = &other.tcp_listeners[addr];

            if my_listener != their_listener {
                v.push(ProxyRequestOrder::RemoveListener(RemoveListener {
                    address: **addr,
                    proxy: ListenerType::TCP,
                }));

                v.push(ProxyRequestOrder::AddTcpListener(their_listener.clone()));
            }

            if *my_active && !*their_active {
                v.push(ProxyRequestOrder::DeactivateListener(DeactivateListener {
                    address: **addr,
                    proxy: ListenerType::TCP,
                    to_scm: false,
                }));
            }

            if !*my_active && *their_active {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: **addr,
                    proxy: ListenerType::TCP,
                    from_scm: false,
                }));
            }
        }

        for addr in my_http_listeners.intersection(&their_http_listeners) {
            let (my_listener, my_active) = &self.http_listeners[addr];
            let (their_listener, their_active) = &other.http_listeners[addr];

            if my_listener != their_listener {
                v.push(ProxyRequestOrder::RemoveListener(RemoveListener {
                    address: **addr,
                    proxy: ListenerType::HTTP,
                }));

                v.push(ProxyRequestOrder::AddHttpListener(their_listener.clone()));
            }

            if *my_active && !*their_active {
                v.push(ProxyRequestOrder::DeactivateListener(DeactivateListener {
                    address: **addr,
                    proxy: ListenerType::HTTP,
                    to_scm: false,
                }));
            }

            if !*my_active && *their_active {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: **addr,
                    proxy: ListenerType::HTTP,
                    from_scm: false,
                }));
            }
        }

        for addr in my_https_listeners.intersection(&their_https_listeners) {
            let (my_listener, my_active) = &self.https_listeners[addr];
            let (their_listener, their_active) = &other.https_listeners[addr];

            if my_listener != their_listener {
                v.push(ProxyRequestOrder::RemoveListener(RemoveListener {
                    address: **addr,
                    proxy: ListenerType::HTTPS,
                }));

                v.push(ProxyRequestOrder::AddHttpsListener(their_listener.clone()));
            }

            if *my_active && !*their_active {
                v.push(ProxyRequestOrder::DeactivateListener(DeactivateListener {
                    address: **addr,
                    proxy: ListenerType::HTTPS,
                    to_scm: false,
                }));
            }

            if !*my_active && *their_active {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: **addr,
                    proxy: ListenerType::HTTPS,
                    from_scm: false,
                }));
            }
        }

        for (cluster_id, res) in diff_map(self.clusters.iter(), other.clusters.iter()) {
            match res {
                DiffResult::Added | DiffResult::Changed => v.push(ProxyRequestOrder::AddCluster(
                    other.clusters.get(cluster_id).unwrap().clone(),
                )),
                DiffResult::Removed => v.push(ProxyRequestOrder::RemoveCluster {
                    cluster_id: cluster_id.to_string(),
                }),
            }
        }

        for ((cluster_id, backend_id), res) in diff_map(
            self.backends.iter().flat_map(|(cluster_id, v)| {
                v.iter()
                    .map(move |backend| ((cluster_id, &backend.backend_id), backend))
            }),
            other.backends.iter().flat_map(|(cluster_id, v)| {
                v.iter()
                    .map(move |backend| ((cluster_id, &backend.backend_id), backend))
            }),
        ) {
            match res {
                DiffResult::Added => {
                    let backend = other
                        .backends
                        .get(cluster_id)
                        .and_then(|v| v.iter().find(|b| &b.backend_id == backend_id))
                        .unwrap();
                    v.push(ProxyRequestOrder::AddBackend(backend.clone()));
                }
                DiffResult::Removed => {
                    let backend = self
                        .backends
                        .get(cluster_id)
                        .and_then(|v| v.iter().find(|b| &b.backend_id == backend_id))
                        .unwrap();

                    v.push(ProxyRequestOrder::RemoveBackend(RemoveBackend {
                        cluster_id: backend.cluster_id.clone(),
                        backend_id: backend.backend_id.clone(),
                        address: backend.address,
                    }));
                }
                DiffResult::Changed => {
                    let backend = self
                        .backends
                        .get(cluster_id)
                        .and_then(|v| v.iter().find(|b| &b.backend_id == backend_id))
                        .unwrap();

                    v.push(ProxyRequestOrder::RemoveBackend(RemoveBackend {
                        cluster_id: backend.cluster_id.clone(),
                        backend_id: backend.backend_id.clone(),
                        address: backend.address,
                    }));

                    let backend = other
                        .backends
                        .get(cluster_id)
                        .and_then(|v| v.iter().find(|b| &b.backend_id == backend_id))
                        .unwrap();
                    v.push(ProxyRequestOrder::AddBackend(backend.clone()));
                }
            }
        }

        let mut my_http_fronts: HashSet<(&RouteKey, &HttpFrontend)> = HashSet::new();
        for (route, front) in self.http_fronts.iter() {
            my_http_fronts.insert((route, front));
        }
        let mut their_http_fronts: HashSet<(&RouteKey, &HttpFrontend)> = HashSet::new();
        for (route, front) in other.http_fronts.iter() {
            their_http_fronts.insert((route, front));
        }

        let removed_http_fronts = my_http_fronts.difference(&their_http_fronts);
        let added_http_fronts = their_http_fronts.difference(&my_http_fronts);

        for &(_, front) in removed_http_fronts {
            v.push(ProxyRequestOrder::RemoveHttpFrontend(front.clone()));
        }

        for &(_, front) in added_http_fronts {
            v.push(ProxyRequestOrder::AddHttpFrontend(front.clone()));
        }

        let mut my_https_fronts: HashSet<(&RouteKey, &HttpFrontend)> = HashSet::new();
        for (route, front) in self.https_fronts.iter() {
            my_https_fronts.insert((route, front));
        }
        let mut their_https_fronts: HashSet<(&RouteKey, &HttpFrontend)> = HashSet::new();
        for (route, front) in other.https_fronts.iter() {
            their_https_fronts.insert((route, front));
        }
        let removed_https_fronts = my_https_fronts.difference(&their_https_fronts);
        let added_https_fronts = their_https_fronts.difference(&my_https_fronts);

        for &(_, front) in removed_https_fronts {
            v.push(ProxyRequestOrder::RemoveHttpsFrontend(front.clone()));
        }

        for &(_, front) in added_https_fronts {
            v.push(ProxyRequestOrder::AddHttpsFrontend(front.clone()));
        }

        let mut my_tcp_fronts: HashSet<(&ClusterId, &TcpFrontend)> = HashSet::new();
        for (cluster_id, front_list) in self.tcp_fronts.iter() {
            for front in front_list.iter() {
                my_tcp_fronts.insert((cluster_id, front));
            }
        }
        let mut their_tcp_fronts: HashSet<(&ClusterId, &TcpFrontend)> = HashSet::new();
        for (cluster_id, front_list) in other.tcp_fronts.iter() {
            for front in front_list.iter() {
                their_tcp_fronts.insert((cluster_id, front));
            }
        }

        let removed_tcp_fronts = my_tcp_fronts.difference(&their_tcp_fronts);
        let added_tcp_fronts = their_tcp_fronts.difference(&my_tcp_fronts);

        for &(_, front) in removed_tcp_fronts {
            v.push(ProxyRequestOrder::RemoveTcpFrontend(front.clone()));
        }

        for &(_, front) in added_tcp_fronts {
            v.push(ProxyRequestOrder::AddTcpFrontend(front.clone()));
        }

        //pub certificates:    HashMap<SocketAddr, HashMap<CertificateFingerprint, (CertificateAndKey, Vec<String>)>>,
        let my_certificates: HashSet<(SocketAddr, &CertificateFingerprint)> = HashSet::from_iter(
            self.certificates
                .iter()
                .flat_map(|(addr, certs)| repeat(*addr).zip(certs.keys())),
        );
        let their_certificates: HashSet<(SocketAddr, &CertificateFingerprint)> = HashSet::from_iter(
            other
                .certificates
                .iter()
                .flat_map(|(addr, certs)| repeat(*addr).zip(certs.keys())),
        );

        let removed_certificates = my_certificates.difference(&their_certificates);
        let added_certificates = their_certificates.difference(&my_certificates);

        for &(address, fingerprint) in removed_certificates {
            v.push(ProxyRequestOrder::RemoveCertificate(RemoveCertificate {
                address,
                fingerprint: fingerprint.clone(),
            }));
        }

        for &(address, fingerprint) in added_certificates {
            if let Some((ref certificate_and_key, ref names)) = other
                .certificates
                .get(&address)
                .and_then(|certs| certs.get(fingerprint))
            {
                v.push(ProxyRequestOrder::AddCertificate(AddCertificate {
                    address,
                    certificate: certificate_and_key.clone(),
                    names: names.clone(),
                    expired_at: None,
                }));
            }
        }

        for address in added_tcp_listeners {
            let listener = &other.tcp_listeners[address];
            if listener.1 {
                v.push(ProxyRequestOrder::ActivateListener(ActivateListener {
                    address: listener.0.address,
                    proxy: ListenerType::TCP,
                    from_scm: false,
                }));
            }
        }

        v
    }

    // FIXME: what about deny rules?
    pub fn hash_state(&self) -> BTreeMap<ClusterId, u64> {
        let mut h: HashMap<_, _> = self
            .clusters
            .keys()
            .map(|cluster_id| {
                let mut s = DefaultHasher::new();
                self.clusters.get(cluster_id).hash(&mut s);
                if let Some(v) = self.backends.get(cluster_id) {
                    v.iter().collect::<BTreeSet<_>>().hash(&mut s)
                }
                if let Some(v) = self.tcp_fronts.get(cluster_id) {
                    v.iter().collect::<BTreeSet<_>>().hash(&mut s)
                }
                (cluster_id.to_string(), s)
            })
            .collect();

        for front in self.http_fronts.values() {
            if let Route::ClusterId(cluster_id) = &front.route {
                if let Some(s) = h.get_mut(cluster_id) {
                    front.hash(s);
                }
            }
        }

        for front in self.https_fronts.values() {
            if let Route::ClusterId(cluster_id) = &front.route {
                if let Some(s) = h.get_mut(cluster_id) {
                    front.hash(s);
                }
            }
        }

        h.drain()
            .map(|(cluster_id, hasher)| (cluster_id, hasher.finish()))
            .collect()
    }

    pub fn cluster_state(&self, cluster_id: &str) -> QueryAnswerCluster {
        QueryAnswerCluster {
            configuration: self.clusters.get(cluster_id).cloned(),
            http_frontends: self
                .http_fronts
                .iter()
                .filter_map(|(_k, v)| match &v.route {
                    Route::Deny => None,
                    Route::ClusterId(id) => {
                        if id == cluster_id {
                            Some(v)
                        } else {
                            None
                        }
                    }
                })
                .cloned()
                .collect(),
            https_frontends: self
                .https_fronts
                .iter()
                .filter_map(|(_k, v)| match &v.route {
                    Route::Deny => None,
                    Route::ClusterId(id) => {
                        if id == cluster_id {
                            Some(v)
                        } else {
                            None
                        }
                    }
                })
                .cloned()
                .collect(),
            tcp_frontends: self.tcp_fronts.get(cluster_id).cloned().unwrap_or_default(),
            backends: self.backends.get(cluster_id).cloned().unwrap_or_default(),
        }
    }

    pub fn count_backends(&self) -> usize {
        self.backends.values().fold(0, |acc, v| acc + v.len())
    }

    pub fn count_frontends(&self) -> usize {
        self.http_fronts.values().count()
            + self.https_fronts.values().count()
            + self.tcp_fronts.values().fold(0, |acc, v| acc + v.len())
    }
}

pub fn get_cluster_ids_by_domain(
    state: &ConfigState,
    hostname: String,
    path: Option<String>,
) -> HashSet<ClusterId> {
    let domain_check = |front_hostname: &str,
                        front_path: &PathRule,
                        hostname: &str,
                        path_prefix: &Option<String>|
     -> bool {
        if hostname != front_hostname {
            return false;
        }

        match (&path_prefix, front_path) {
            (None, _) => true,
            (Some(ref path), PathRule::Prefix(s)) => path == s,
            (Some(ref path), PathRule::Regex(s)) => path == s,
            (Some(ref path), PathRule::Equals(s)) => path == s,
        }
    };

    let mut cluster_ids: HashSet<ClusterId> = HashSet::new();

    state.http_fronts.values().for_each(|front| {
        if domain_check(&front.hostname, &front.path, &hostname, &path) {
            if let Route::ClusterId(id) = &front.route {
                cluster_ids.insert(id.to_string());
            }
        }
    });

    state.https_fronts.values().for_each(|front| {
        if domain_check(&front.hostname, &front.path, &hostname, &path) {
            if let Route::ClusterId(id) = &front.route {
                cluster_ids.insert(id.to_string());
            }
        }
    });

    cluster_ids
}

pub fn get_certificate(state: &ConfigState, fingerprint: &[u8]) -> Option<(String, Vec<String>)> {
    state
        .certificates
        .values()
        .filter_map(|h| h.get(&CertificateFingerprint(fingerprint.to_vec())))
        .map(|(c, names)| (c.certificate.clone(), names.clone()))
        .next()
}

struct DiffMap<'a, K: Ord, V, I1, I2> {
    my_it: I1,
    other_it: I2,
    my: Option<(K, &'a V)>,
    other: Option<(K, &'a V)>,
}

//fn diff_map<'a, K:Ord, V: PartialEq>(my: &'a BTreeMap<K,V>, other: &'a BTreeMap<K,V>) -> DiffMap<'a,K,V> {
fn diff_map<
    'a,
    K: Ord,
    V: PartialEq,
    I1: Iterator<Item = (K, &'a V)>,
    I2: Iterator<Item = (K, &'a V)>,
>(
    my: I1,
    other: I2,
) -> DiffMap<'a, K, V, I1, I2> {
    DiffMap {
        my_it: my,
        other_it: other,
        my: None,
        other: None,
    }
}

enum DiffResult {
    Added,
    Removed,
    Changed,
}

// this will iterate over the keys of both iterators
// since keys are sorted, it should be easy to see which ones are in common or not
impl<
        'a,
        K: Ord,
        V: PartialEq,
        I1: Iterator<Item = (K, &'a V)>,
        I2: Iterator<Item = (K, &'a V)>,
    > std::iter::Iterator for DiffMap<'a, K, V, I1, I2>
{
    type Item = (K, DiffResult);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.my.is_none() {
                self.my = self.my_it.next();
            }
            if self.other.is_none() {
                self.other = self.other_it.next();
            }

            match (self.my.take(), self.other.take()) {
                // there are no more elements in my_it, all the next elements in other
                // should be added
                // if other was none, we will stop the iterator there
                (None, other) => return other.map(|(k, _)| (k, DiffResult::Added)),
                // there are no more elements in other_it, all the next elements in my
                // should be removed
                (Some((k, _)), None) => return Some((k, DiffResult::Removed)),
                // element is present in my but not other
                (Some((k1, _v1)), Some((k2, v2))) if k1 < k2 => {
                    self.other = Some((k2, v2));
                    return Some((k1, DiffResult::Removed));
                }
                // element is present in other byt not in my
                (Some((k1, v1)), Some((k2, _v2))) if k1 > k2 => {
                    self.my = Some((k1, v1));
                    return Some((k2, DiffResult::Added));
                }
                (Some((k1, v1)), Some((_k2, v2))) if v1 != v2 => {
                    // key is present in both, if elements have changed
                    // return a value, otherwise go to the next key for both maps
                    return Some((k1, DiffResult::Changed));
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{
        Backend, HttpFrontend, LoadBalancingAlgorithms, LoadBalancingParams, PathRule,
        ProxyRequestOrder, Route, RulePosition, TlsProvider,
    };

    #[test]
    fn serialize() {
        let mut state: ConfigState = Default::default();
        state.handle_order(&ProxyRequestOrder::AddHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("cluster_1")),
            hostname: String::from("lolcatho.st:8080"),
            path: PathRule::Prefix(String::from("/")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("cluster_2")),
            hostname: String::from("test.local"),
            path: PathRule::Prefix(String::from("/abc")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Pre,
            tags: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-0"),
            address: "127.0.0.1:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-1"),
            address: "127.0.0.2:1027".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_2"),
            backend_id: String::from("cluster_2-0"),
            address: "192.167.1.2:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-3"),
            address: "192.168.1.3:1027".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::RemoveBackend(RemoveBackend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-3"),
            address: "192.168.1.3:1027".parse().unwrap(),
        }));

        /*
        let encoded = state.encode();
        println!("serialized:\n{}", encoded);

        let new_state: Option<HttpProxy> = decode_str(&encoded);
        println!("deserialized:\n{:?}", new_state);
        assert_eq!(new_state, Some(state));
        */
        //assert!(false);
    }

    #[test]
    fn diff() {
        let mut state: ConfigState = Default::default();
        state.handle_order(&ProxyRequestOrder::AddHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("cluster_1")),
            hostname: String::from("lolcatho.st:8080"),
            path: PathRule::Prefix(String::from("/")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Post,
            tags: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("cluster_2")),
            hostname: String::from("test.local"),
            path: PathRule::Prefix(String::from("/abc")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-0"),
            address: "127.0.0.1:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-1"),
            address: "127.0.0.2:1027".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_2"),
            backend_id: String::from("cluster_2-0"),
            address: "192.167.1.2:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state.handle_order(&ProxyRequestOrder::AddCluster(Cluster {
            cluster_id: String::from("cluster_2"),
            sticky_session: true,
            https_redirect: true,
            proxy_protocol: None,
            load_balancing: LoadBalancingAlgorithms::RoundRobin,
            load_metric: None,
            answer_503: None,
        }));

        let mut state2: ConfigState = Default::default();
        state2.handle_order(&ProxyRequestOrder::AddHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("cluster_1")),
            hostname: String::from("lolcatho.st:8080"),
            path: PathRule::Prefix(String::from("/")),
            address: "0.0.0.0:8080".parse().unwrap(),
            method: None,
            position: RulePosition::Post,
            tags: None,
        }));
        state2.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-0"),
            address: "127.0.0.1:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state2.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-1"),
            address: "127.0.0.2:1027".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state2.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-2"),
            address: "127.0.0.2:1028".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        state2.handle_order(&ProxyRequestOrder::AddCluster(Cluster {
            cluster_id: String::from("cluster_3"),
            sticky_session: false,
            https_redirect: false,
            proxy_protocol: None,
            load_balancing: LoadBalancingAlgorithms::RoundRobin,
            load_metric: None,
            answer_503: None,
        }));

        let e = vec![
            ProxyRequestOrder::RemoveHttpFrontend(HttpFrontend {
                route: Route::ClusterId(String::from("cluster_2")),
                hostname: String::from("test.local"),
                path: PathRule::Prefix(String::from("/abc")),
                method: None,
                address: "0.0.0.0:8080".parse().unwrap(),
                position: RulePosition::Tree,
                tags: None,
            }),
            ProxyRequestOrder::RemoveBackend(RemoveBackend {
                cluster_id: String::from("cluster_2"),
                backend_id: String::from("cluster_2-0"),
                address: "192.167.1.2:1026".parse().unwrap(),
            }),
            ProxyRequestOrder::AddBackend(Backend {
                cluster_id: String::from("cluster_1"),
                backend_id: String::from("cluster_1-2"),
                address: "127.0.0.2:1028".parse().unwrap(),
                load_balancing_parameters: Some(LoadBalancingParams::default()),
                sticky_id: None,
                backup: None,
            }),
            ProxyRequestOrder::RemoveCluster {
                cluster_id: String::from("cluster_2"),
            },
            ProxyRequestOrder::AddCluster(Cluster {
                cluster_id: String::from("cluster_3"),
                sticky_session: false,
                https_redirect: false,
                proxy_protocol: None,
                load_balancing: LoadBalancingAlgorithms::RoundRobin,
                load_metric: None,
                answer_503: None,
            }),
        ];
        let expected_diff: HashSet<&ProxyRequestOrder> = HashSet::from_iter(e.iter());

        let d = state.diff(&state2);
        let diff = HashSet::from_iter(d.iter());
        println!("diff orders:\n{:#?}\n", diff);
        println!("expected diff orders:\n{:#?}\n", expected_diff);

        let hash1 = state.hash_state();
        let hash2 = state2.hash_state();
        let mut state3 = state.clone();
        state3.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-2"),
            address: "127.0.0.2:1028".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));
        let hash3 = state3.hash_state();
        println!("state 1 hashes: {:#?}", hash1);
        println!("state 2 hashes: {:#?}", hash2);
        println!("state 3 hashes: {:#?}", hash3);

        assert_eq!(diff, expected_diff);
    }

    #[test]
    fn cluster_ids_by_domain() {
        let mut config = ConfigState::new();
        let http_front_cluster1 = HttpFrontend {
            route: Route::ClusterId(String::from("MyCluster_1")),
            hostname: String::from("lolcatho.st"),
            path: PathRule::Prefix(String::from("")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        };

        let https_front_cluster1 = HttpFrontend {
            route: Route::ClusterId(String::from("MyCluster_1")),
            hostname: String::from("lolcatho.st"),
            path: PathRule::Prefix(String::from("")),
            method: None,
            address: "0.0.0.0:8443".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        };

        let http_front_cluster2 = HttpFrontend {
            route: Route::ClusterId(String::from("MyCluster_2")),
            hostname: String::from("lolcatho.st"),
            path: PathRule::Prefix(String::from("/api")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        };

        let https_front_cluster2 = HttpFrontend {
            route: Route::ClusterId(String::from("MyCluster_2")),
            hostname: String::from("lolcatho.st"),
            path: PathRule::Prefix(String::from("/api")),
            method: None,
            address: "0.0.0.0:8443".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        };

        let add_http_front_order_cluster1 = ProxyRequestOrder::AddHttpFrontend(http_front_cluster1);
        let add_http_front_order_cluster2 = ProxyRequestOrder::AddHttpFrontend(http_front_cluster2);
        let add_https_front_order_cluster1 =
            ProxyRequestOrder::AddHttpsFrontend(https_front_cluster1);
        let add_https_front_order_cluster2 =
            ProxyRequestOrder::AddHttpsFrontend(https_front_cluster2);
        config.handle_order(&add_http_front_order_cluster1);
        config.handle_order(&add_http_front_order_cluster2);
        config.handle_order(&add_https_front_order_cluster1);
        config.handle_order(&add_https_front_order_cluster2);

        let mut cluster1_cluster2: HashSet<ClusterId> = HashSet::new();
        cluster1_cluster2.insert(String::from("MyCluster_1"));
        cluster1_cluster2.insert(String::from("MyCluster_2"));

        let mut cluster2: HashSet<ClusterId> = HashSet::new();
        cluster2.insert(String::from("MyCluster_2"));

        let empty: HashSet<ClusterId> = HashSet::new();
        assert_eq!(
            get_cluster_ids_by_domain(&config, String::from("lolcatho.st"), None),
            cluster1_cluster2
        );
        assert_eq!(
            get_cluster_ids_by_domain(
                &config,
                String::from("lolcatho.st"),
                Some(String::from("/api"))
            ),
            cluster2
        );
        assert_eq!(
            get_cluster_ids_by_domain(&config, String::from("lolcathost"), None),
            empty
        );
        assert_eq!(
            get_cluster_ids_by_domain(
                &config,
                String::from("lolcathost"),
                Some(String::from("/sozu"))
            ),
            empty
        );
    }

    #[test]
    fn duplicate_backends() {
        let mut state: ConfigState = Default::default();
        state.handle_order(&ProxyRequestOrder::AddBackend(Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-0"),
            address: "127.0.0.1:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: None,
            backup: None,
        }));

        let b = Backend {
            cluster_id: String::from("cluster_1"),
            backend_id: String::from("cluster_1-0"),
            address: "127.0.0.1:1026".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams::default()),
            sticky_id: Some("sticky".to_string()),
            backup: None,
        };

        state.handle_order(&ProxyRequestOrder::AddBackend(b.clone()));

        assert_eq!(state.backends.get("cluster_1").unwrap(), &vec![b]);
    }

    #[test]
    fn listener_diff() {
        let mut state: ConfigState = Default::default();
        state.handle_order(&ProxyRequestOrder::AddTcpListener(TcpListener {
            address: "0.0.0.0:1234".parse().unwrap(),
            public_address: None,
            expect_proxy: false,
            front_timeout: 60,
            back_timeout: 30,
            connect_timeout: 3,
        }));
        state.handle_order(&ProxyRequestOrder::ActivateListener(ActivateListener {
            address: "0.0.0.0:1234".parse().unwrap(),
            proxy: ListenerType::TCP,
            from_scm: false,
        }));
        state.handle_order(&ProxyRequestOrder::AddHttpListener(HttpListener {
            address: "0.0.0.0:8080".parse().unwrap(),
            public_address: None,
            expect_proxy: false,
            answer_404: String::new(),
            answer_503: String::new(),
            sticky_name: String::new(),
            front_timeout: 60,
            request_timeout: 10,
            back_timeout: 30,
            connect_timeout: 3,
        }));
        state.handle_order(&ProxyRequestOrder::AddHttpsListener(HttpsListener {
            address: "0.0.0.0:8443".parse().unwrap(),
            public_address: None,
            expect_proxy: false,
            answer_404: String::new(),
            answer_503: String::new(),
            sticky_name: String::new(),
            versions: vec![],
            cipher_list: vec![],
            cipher_suites: vec![],
            signature_algorithms: vec![],
            groups_list: vec![],
            tls_provider: TlsProvider::Openssl,
            certificate: None,
            certificate_chain: vec![],
            key: None,
            front_timeout: 60,
            request_timeout: 10,
            back_timeout: 30,
            connect_timeout: 3,
        }));
        state.handle_order(&ProxyRequestOrder::ActivateListener(ActivateListener {
            address: "0.0.0.0:8443".parse().unwrap(),
            proxy: ListenerType::HTTPS,
            from_scm: false,
        }));

        let mut state2: ConfigState = Default::default();
        state2.handle_order(&ProxyRequestOrder::AddTcpListener(TcpListener {
            address: "0.0.0.0:1234".parse().unwrap(),
            public_address: None,
            expect_proxy: true,
            front_timeout: 60,
            back_timeout: 30,
            connect_timeout: 3,
        }));
        state2.handle_order(&ProxyRequestOrder::AddHttpListener(HttpListener {
            address: "0.0.0.0:8080".parse().unwrap(),
            public_address: None,
            expect_proxy: false,
            answer_404: "test".to_string(),
            answer_503: String::new(),
            sticky_name: String::new(),
            front_timeout: 60,
            request_timeout: 10,
            back_timeout: 30,
            connect_timeout: 3,
        }));
        state2.handle_order(&ProxyRequestOrder::ActivateListener(ActivateListener {
            address: "0.0.0.0:8080".parse().unwrap(),
            proxy: ListenerType::HTTP,
            from_scm: false,
        }));
        state2.handle_order(&ProxyRequestOrder::AddHttpsListener(HttpsListener {
            address: "0.0.0.0:8443".parse().unwrap(),
            public_address: None,
            expect_proxy: false,
            answer_404: String::from("test"),
            answer_503: String::new(),
            sticky_name: String::new(),
            versions: vec![],
            cipher_list: vec![],
            cipher_suites: vec![],
            signature_algorithms: vec![],
            groups_list: vec![],
            tls_provider: TlsProvider::Openssl,
            certificate: None,
            certificate_chain: vec![],
            key: None,
            front_timeout: 60,
            request_timeout: 10,
            back_timeout: 30,
            connect_timeout: 3,
        }));
        state2.handle_order(&ProxyRequestOrder::ActivateListener(ActivateListener {
            address: "0.0.0.0:8443".parse().unwrap(),
            proxy: ListenerType::HTTPS,
            from_scm: false,
        }));

        let e = vec![
            ProxyRequestOrder::RemoveListener(RemoveListener {
                address: "0.0.0.0:1234".parse().unwrap(),
                proxy: ListenerType::TCP,
            }),
            ProxyRequestOrder::AddTcpListener(TcpListener {
                address: "0.0.0.0:1234".parse().unwrap(),
                public_address: None,
                expect_proxy: true,
                front_timeout: 60,
                back_timeout: 30,
                connect_timeout: 3,
            }),
            ProxyRequestOrder::DeactivateListener(DeactivateListener {
                address: "0.0.0.0:1234".parse().unwrap(),
                proxy: ListenerType::TCP,
                to_scm: false,
            }),
            ProxyRequestOrder::RemoveListener(RemoveListener {
                address: "0.0.0.0:8080".parse().unwrap(),
                proxy: ListenerType::HTTP,
            }),
            ProxyRequestOrder::AddHttpListener(HttpListener {
                address: "0.0.0.0:8080".parse().unwrap(),
                public_address: None,
                expect_proxy: false,
                answer_404: String::from("test"),
                answer_503: String::new(),
                sticky_name: String::new(),
                front_timeout: 60,
                request_timeout: 10,
                back_timeout: 30,
                connect_timeout: 3,
            }),
            ProxyRequestOrder::ActivateListener(ActivateListener {
                address: "0.0.0.0:8080".parse().unwrap(),
                proxy: ListenerType::HTTP,
                from_scm: false,
            }),
            ProxyRequestOrder::RemoveListener(RemoveListener {
                address: "0.0.0.0:8443".parse().unwrap(),
                proxy: ListenerType::HTTPS,
            }),
            ProxyRequestOrder::AddHttpsListener(HttpsListener {
                address: "0.0.0.0:8443".parse().unwrap(),
                public_address: None,
                expect_proxy: false,
                answer_404: String::from("test"),
                answer_503: String::new(),
                sticky_name: String::new(),
                versions: vec![],
                cipher_list: vec![],
                cipher_suites: vec![],
                signature_algorithms: vec![],
                groups_list: vec![],
                tls_provider: TlsProvider::Openssl,
                certificate: None,
                certificate_chain: vec![],
                key: None,
                front_timeout: 60,
                request_timeout: 10,
                back_timeout: 30,
                connect_timeout: 3,
            }),
        ];

        let diff = state.diff(&state2);
        //let diff: HashSet<&ProxyRequestOrder> = HashSet::from_iter(d.iter());
        println!("expected diff orders:\n{:#?}\n", e);
        println!("diff orders:\n{:#?}\n", diff);

        let _hash1 = state.hash_state();
        let _hash2 = state2.hash_state();

        assert_eq!(diff, e);
    }
}

/// `RouteKey` is a the routing key built from the following tuple.
/// The tuple is made of (socket address, hostname, path, method).
// TODO: Create a custom type for the hostname and use a common type for the method.
#[derive(PartialOrd, Ord, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteKey(pub SocketAddr, pub String, pub PathRule, pub Option<String>);

impl serde::Serialize for RouteKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = match &self.2 {
            PathRule::Prefix(prefix) => format!("{};{};P{}", self.0, self.1, prefix),
            PathRule::Regex(regex) => format!("{};{};R{}", self.0, self.1, regex),
            PathRule::Equals(path) => format!("{};{};={}", self.0, self.1, path),
        };

        if let Some(method) = &self.3 {
            s = format!("{};{}", s, method);
        }

        serializer.serialize_str(&s)
    }
}

impl From<HttpFrontend> for RouteKey {
    fn from(frontend: HttpFrontend) -> Self {
        Self(
            frontend.address,
            frontend.hostname,
            frontend.path,
            frontend.method,
        )
    }
}

struct RouteKeyVisitor {}

impl<'de> Visitor<'de> for RouteKeyVisitor {
    type Value = RouteKey;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("parsing a route key")
    }

    fn visit_str<E>(self, value: &str) -> Result<RouteKey, E>
    where
        E: de::Error,
    {
        let mut it = value.split(';');
        let address = it
            .next()
            .ok_or_else(|| E::custom("invalid format".to_string()))
            .and_then(|s| {
                s.parse::<SocketAddr>()
                    .map_err(|e| E::custom(format!("could not deserialize SocketAddr: {:?}", e)))
            })?;

        let hostname = it
            .next()
            .ok_or_else(|| E::custom("invalid format".to_string()))?;

        let path_rule_str = it
            .next()
            .ok_or_else(|| E::custom("invalid format".to_string()))?;

        let path_rule = match path_rule_str.chars().next() {
            Some('R') => PathRule::Regex(String::from(&path_rule_str[1..])),
            Some('P') => PathRule::Prefix(String::from(&path_rule_str[1..])),
            Some('=') => PathRule::Equals(String::from(&path_rule_str[1..])),
            _ => return Err(E::custom("invalid path rule".to_string())),
        };

        let method = it.next().map(String::from);

        Ok(RouteKey(address, hostname.to_string(), path_rule, method))
    }
}

impl<'de> serde::Deserialize<'de> for RouteKey {
    fn deserialize<D>(deserializer: D) -> Result<RouteKey, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(RouteKeyVisitor {})
    }
}
