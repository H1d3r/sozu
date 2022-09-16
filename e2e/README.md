# End to end tests

We want to check Sōzu's behavior in all corner cases for the CI.

This creates mocked backends with simple aggregators to monitor their behavior, and uses Sōzu lib directly.

    cd e2e/
    cargo run
