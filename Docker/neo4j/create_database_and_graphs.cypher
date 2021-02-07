CREATE CONSTRAINT articleIdConstraint IF NOT EXISTS ON (article:Article) ASSERT article.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///articles.csv' AS row
MERGE (a:Article {id: row.id, publication: row.publication, title: row.title, url: row.url})
