from feedback_lakehouse.sentiment import score_aspects


def test_scores_each_aspect_from_its_own_clause() -> None:
    results = {
        item.aspect: item
        for item in score_aspects("The camera is excellent, but the battery drains quickly.")
    }
    assert results["camera"].sentiment_label == "positive"
    assert results["battery"].sentiment_label == "negative"
    assert "camera" in results["camera"].evidence.lower()
    assert "battery" in results["battery"].evidence.lower()


def test_unmentioned_aspects_are_not_invented() -> None:
    assert score_aspects("Nothing relevant is discussed here.") == []
