INDEX_SCHEMA = '''
{
  "settings": {
    "refresh_interval": "1s",
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_stemmer": {
          "type": "stemmer",
          "language": "russian"
        }
      },
      "analyzer": {
        "ru_en": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "english_stemmer",
            "russian_stop",
            "russian_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
      "title": {
        "type": "text"
      },
      "description": {
        "type": "text"
      },
      "type": {
        "type": "keyword"
      },
      "genres": {
        "type": "keyword"
      },
      "rating": {
        "type": "float"
      },
      "creation_date": {
        "type": "date"
      },
      "certificate": {
        "type": "keyword"
      },
      "age_limit": {
        "type": "keyword"
      },
      "file_path": {
        "type": "keyword"
      },
      "directors": {
        "type": "text"
      },
      "actors": {
        "type": "text"
      },
      "writers": {
        "type": "text"
      }
    }
  }
}
'''
