use agent_economy_monitor::classification_promotion::{PromotionError, PromotionRequest};

#[test]
fn promotion_request_requires_complete_bounded_authority() {
    let request = PromotionRequest::new(
        "buyer:one",
        "run:baseline",
        1,
        "operator-review",
        "00000000-0000-0000-0000-000000000010",
    )
    .unwrap();
    assert_eq!(request.run_version(), 1);

    assert_eq!(
        PromotionRequest::new(
            "buyer:one",
            "run:baseline",
            0,
            "operator-review",
            "00000000-0000-0000-0000-000000000010",
        ),
        Err(PromotionError::InvalidRequest)
    );
    assert_eq!(
        PromotionRequest::new(
            "buyer:one",
            "run:baseline",
            1,
            " ",
            "00000000-0000-0000-0000-000000000010",
        ),
        Err(PromotionError::InvalidRequest)
    );
}

#[test]
fn promotion_command_requires_exactly_five_arguments() {
    let request = PromotionRequest::from_args([
        "buyer:one",
        "run:baseline",
        "2",
        "operator-review",
        "00000000-0000-0000-0000-000000000010",
    ])
    .unwrap();
    assert_eq!(request.run_version(), 2);

    assert_eq!(
        PromotionRequest::from_args(["buyer:one", "run:baseline"]),
        Err(PromotionError::InvalidRequest)
    );
}
