import random

import pytest

from quoit import RetryPolicy


def test_default_policy_never_retries():
    policy = RetryPolicy()

    assert policy.max_attempts == 1
    assert not policy.should_retry(ValueError("x"), 1)


def test_none_matches_the_default():
    assert RetryPolicy.none() == RetryPolicy()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_delay": -1},
        {"max_delay": -1},
        {"factor": 0.5},
        {"jitter": 1.5},
        {"jitter": -0.1},
    ],
)
def test_invalid_settings_are_rejected(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_fixed_delay_is_constant():
    policy = RetryPolicy.fixed(5, 0.25)

    assert [policy.delay_for(n) for n in range(1, 5)] == [0.25, 0.25, 0.25, 0.25]


def test_exponential_delay_doubles():
    policy = RetryPolicy.exponential(6, 1.0)

    assert [policy.delay_for(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_custom_factor():
    policy = RetryPolicy.exponential(4, 0.5, factor=3)

    assert [policy.delay_for(n) for n in range(1, 4)] == [0.5, 1.5, 4.5]


def test_delay_is_capped():
    policy = RetryPolicy.exponential(10, 1.0, max_delay=5.0)

    assert policy.delay_for(8) == 5.0


def test_delay_for_rejects_zero_attempts():
    with pytest.raises(ValueError):
        RetryPolicy.fixed(3, 1.0).delay_for(0)


def test_jitter_stays_within_its_band():
    policy = RetryPolicy(max_attempts=3, base_delay=10.0, factor=1.0, max_delay=100.0, jitter=0.2)
    rng = random.Random(7)
    delays = [policy.delay_for(1, rng) for _ in range(200)]

    assert all(8.0 <= d <= 12.0 for d in delays)
    assert len(set(delays)) > 50


def test_jitter_is_repeatable_with_a_seeded_rng():
    policy = RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.5)

    first = [policy.delay_for(n, random.Random(1)) for n in (1, 2)]
    second = [policy.delay_for(n, random.Random(1)) for n in (1, 2)]

    assert first == second


def test_jitter_never_exceeds_max_delay():
    policy = RetryPolicy(max_attempts=3, base_delay=10.0, factor=1.0, max_delay=10.0, jitter=1.0)
    rng = random.Random(3)

    assert all(policy.delay_for(1, rng) <= 10.0 for _ in range(100))


def test_zero_base_delay_means_immediate_retries():
    assert RetryPolicy(max_attempts=3).delay_for(2) == 0.0


def test_should_retry_stops_at_max_attempts():
    policy = RetryPolicy.fixed(3, 0.0)

    assert policy.should_retry(RuntimeError(), 1)
    assert policy.should_retry(RuntimeError(), 2)
    assert not policy.should_retry(RuntimeError(), 3)


def test_retry_on_limits_which_errors_retry():
    policy = RetryPolicy(max_attempts=3, retry_on=(ConnectionError,))

    assert policy.should_retry(ConnectionError(), 1)
    assert not policy.should_retry(ValueError(), 1)


def test_give_up_on_beats_retry_on():
    policy = RetryPolicy(max_attempts=3, retry_on=(OSError,), give_up_on=(PermissionError,))

    assert policy.should_retry(ConnectionError(), 1)
    assert not policy.should_retry(PermissionError(), 1)


def test_policies_are_immutable():
    policy = RetryPolicy.fixed(2, 1.0)

    with pytest.raises(AttributeError):
        policy.max_attempts = 9
