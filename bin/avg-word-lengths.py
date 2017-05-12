

import click

from pyspark.sql import SparkSession, Row, types as T
from pyspark import SparkContext


SCHEMA = T.StructType([
    T.StructField('book_id', T.StringType()),
    T.StructField('title', T.StringType()),
    T.StructField('author_first', T.StringType()),
    T.StructField('author_last', T.StringType()),
    T.StructField('avg_len', T.DoubleType()),
])


def avg_word_lengths(row):
    """Given a novel, get the average length of words.
    """
    lens = list(map(lambda t: len(t.token), row.tokens))

    avg_len = sum(lens) / len(lens)

    return Row(
        book_id=row.bookId,
        title=row.title,
        author_first=row.authFirst,
        author_last=row.authLast,
        avg_len=avg_len,
    )


@click.command()
@click.argument('in_path', type=click.Path())
@click.argument('out_path', type=click.Path())
def main(in_path, out_path):
    """Get the average word length for each novel.
    """
    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    novels = spark.read.parquet(in_path)

    result = novels.limit(10).rdd.map(avg_word_lengths)

    df = spark.createDataFrame(result, SCHEMA)
    df.write.json(out_path)


if __name__ == '__main__':
    main()
