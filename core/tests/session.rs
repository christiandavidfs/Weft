use std::time::{Duration, Instant};

use weft_core::network::NetworkEngine;

fn wait_until<F>(timeout: Duration, mut pred: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    false
}

#[test]fn two_devices_form_session_and_hand_transmit_token() {
    let a = NetworkEngine::start("Alpha".to_string()).unwrap();
    let b = NetworkEngine::start("Beta".to_string()).unwrap();

    let sa = || a.status();
    let sb = || b.status();

    // 1. Deben converger a un coordinador + un miembro, con ambos en la lista de miembros.
    let formed = wait_until(Duration::from_secs(15), || {
        let (ra, rb) = (sa().role, sb().role);
        let (ra, rb) = (ra.as_str(), rb.as_str());
        let both_members = sa().members.len() == 2 && sb().members.len() == 2;
        ((ra == "coordinator" && rb == "member") || (ra == "member" && rb == "coordinator"))
            && both_members
    });
    assert!(formed, "no se formó la sesión: A={:?} B={:?}", sa(), sb());

    // 2. El coordinador toma el token de transmisión.
    let (coord, member): (&NetworkEngine, &NetworkEngine) = if sa().role == "coordinator" {
        (&a, &b)
    } else {
        (&b, &a)
    };
    coord.request_transmit();
    let granted = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == coord.status().device_id
            && sb().transmitter_id == coord.status().device_id
    });
    assert!(granted, "el coordinador no obtuvo el token: {:?}", sa());

    // 3. El miembro pide el token (el coordinador está ocupado) → queda en espera.
    member.request_transmit();
    let waiting = wait_until(Duration::from_secs(5), || {
        coord.status().pending_transmit_requests.contains(&member.status().device_id)
    });
    assert!(waiting, "el miembro no quedó en espera: {:?}", coord.status());

    // 4. El coordinador libera el token → el miembro pasa a transmitir automáticamente.
    coord.release_transmit();
    let handed = wait_until(Duration::from_secs(5), || {
        sa().transmitter_id == member.status().device_id
            && sb().transmitter_id == member.status().device_id
    });
    assert!(handed, "el token no pasó al miembro: {:?} {:?}", sa(), sb());

    a.stop();
    b.stop();
}
