from truflation.data.ingestors.happy_ingestor import HappyIngestor
import pandas as pd


def test_happy_ingestor():
    df = pd.DataFrame({
        'country': ['foo', 'bar', 'foo', 'bar'],
        'year': ['2000', '2000', '2024', '2024'],
        'Life Ladder': ['1', '1', '2', '2'],

    })
    ingestor = HappyIngestor()
    result = ingestor.process(df)
    assert result.to() == '{"2000":2.0,"2024":4.0}'

