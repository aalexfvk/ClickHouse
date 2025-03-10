#include <IO/S3/Client.h>
#include <IO/S3Common.h>

#include <gtest/gtest.h>

#if USE_AWS_S3


TEST(RetryStrategy, Main)
{
    uint32_t max_retries = 10;
    uint32_t scale_factor = 100;
    uint32_t max_delay = 1000;

    DB::S3::Client::RetryStrategy retry_strategy(max_retries, scale_factor, max_delay);

    EXPECT_EQ(retry_strategy.GetMaxAttempts(), max_retries + 1);

    Aws::Client::AWSError<Aws::Client::CoreErrors> error{Aws::Client::CoreErrors::INTERNAL_FAILURE, true};
    Aws::Client::AWSError<Aws::Client::CoreErrors> non_retryable_error{Aws::Client::CoreErrors::INTERNAL_FAILURE, false};

    EXPECT_EQ(retry_strategy.ShouldRetry(non_retryable_error, /* attemptedRetries = */ 1), false);

    EXPECT_EQ(retry_strategy.ShouldRetry(error, 1), true);
    EXPECT_EQ(retry_strategy.ShouldRetry(error, max_retries - 1), true);
    EXPECT_EQ(retry_strategy.ShouldRetry(error, max_retries), false);

    EXPECT_EQ(retry_strategy.CalculateDelayBeforeNextRetry(error, 1), 200);
    EXPECT_EQ(retry_strategy.CalculateDelayBeforeNextRetry(error, 2), 400);
    EXPECT_EQ(retry_strategy.CalculateDelayBeforeNextRetry(error, max_retries), 1000);
}

TEST(RetryStrategy, StrategyPerError)
{
    uint32_t max_retries = 10;
    uint32_t scale_factor = 100;
    uint32_t max_delay = 1000;

    DB::S3::Client::RetryStrategy retry_strategy;
    Aws::Client::AWSError<Aws::Client::CoreErrors> error{Aws::Client::CoreErrors::INTERNAL_FAILURE, false};

    retry_strategy.SetStrategyPerError(error.GetErrorType(), max_retries, scale_factor, max_delay);

    for (int attempt = 1; attempt < max_retries; attempt++)
    {
        retry_strategy.RequestBookkeeping(Aws::Client::HttpResponseOutcome(error));

        EXPECT_EQ(retry_strategy.ShouldRetry(error, 0), true)
            << fmt::format("Should retry on attempt: {} with max_retries: {}", attempt, max_retries);

        uint64_t backoff_limited_pow = 1ul << std::min(attempt, 31);
        uint64_t expected_delay = std::min<uint64_t>(scale_factor * backoff_limited_pow, max_delay);

        EXPECT_EQ(retry_strategy.CalculateDelayBeforeNextRetry(error, attempt), expected_delay);
    }

    retry_strategy.RequestBookkeeping(Aws::Client::HttpResponseOutcome(error));
    EXPECT_EQ(retry_strategy.ShouldRetry(error, 0), false);
}

#endif
