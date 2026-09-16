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


def test_aliases_match_whole_words_not_substrings() -> None:
    # Each of these used to invent an aspect: "because" contains "use",
    # "flag" contains "lag", "breakfast" contains "fast", "costume" contains "cost".
    assert [r.aspect for r in score_aspects("I bought it because the photos are great.")] == [
        "camera"
    ]
    assert [r.aspect for r in score_aspects("The flag on the box was torn.")] == ["packaging"]
    assert score_aspects("Breakfast was included and the costume fits.") == []
    assert [r.aspect for r in score_aspects("The mouse arrived in a house-shaped box.")] == [
        "delivery",
        "packaging",
    ]


def test_aliases_still_match_plurals_and_inflections() -> None:
    aspects = {r.aspect for r in score_aspects("Cameras and photos are excellent, charged fast.")}
    assert aspects == {"battery", "camera", "performance"}
