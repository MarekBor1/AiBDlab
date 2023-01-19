from app import hello
from app import extract_sentiment
from app import text_contain_word
from app import bubble_sort
import pytest

def test_hello():
    got = hello("Aleksandra")
    want = "Hello Aleksandra"
    assert got == want


testdata1 = ["I think today will be a great day"]

@pytest.mark.parametrize('sample', testdata1)
def test_extract_sentiment(sample):

    sentiment = extract_sentiment(sample)

    assert sentiment > 0


testdata = [
    ('There is a duck in this text', 'duck', True),
    ('There is nothing here', 'duck', False)
    ]

@pytest.mark.parametrize('sample, word, expected_output', testdata)
def test_text_contain_word(sample, word, expected_output):

    assert text_contain_word(word, sample) == expected_output


testdata2=[([2,5,6,7,2,6],[2,2,5,6,6,7])
          ,([1,2,3,4,5,6],[1,2,3,4,5,6])
          ,([5,2,1,5,6],[1,2,5,5,6])]
@pytest.mark.parametrize('example_array, expected_output', testdata2)
def test_bubble_sort(example_array, expected_output):

    assert bubble_sort(example_array) == expected_output