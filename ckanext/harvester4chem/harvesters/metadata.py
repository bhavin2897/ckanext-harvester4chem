from oaipmh.metadata import MetadataReader

oai_ddi_reader = MetadataReader(
    fields={
        "title": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/titlStmt/titl/text()",
        ),  # noqa
        "creator": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/rspStmt/AuthEnty/text()",
        ),  # noqa
        "subject": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/stdyInfo/subject/keyword/text()",
        ),  # noqa
        "description": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/stdyInfo/abstract/text()",
        ),  # noqa
        "publisher": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/distStmt/contact/text()",
        ),  # noqa
        "contributor": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/contributor/text()",
        ),  # noqa
        "date": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/prodStmt/prodDate/text()",
        ),  # noqa
        "series": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/serStmt/serName/text()",
        ),  # noqa
        "type": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/stdyInfo/sumDscr/dataKind/text()",
        ),  # noqa
        "format": (
            "textList",
            "oai_ddi:codeBook/fileDscr/fileType/text()",
        ),  # noqa
        "identifier": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/citation/titlStmt/IDNo/text()",
        ),  # noqa
        "source": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/dataAccs/setAvail/accsPlac/@URI",
        ),  # noqa
        "language": ("textList", "oai_ddi:codeBook/@xml:lang"),  # noqa
        "tempCoverage": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/stdyInfo/sumDscr/timePrd/text()",
        ),  # noqa
        "geoCoverage": (
            "textList",
            "oai_ddi:codeBook/stdyDscr/stdyInfo/sumDscr/geogCover/text()",
        ),  # noqa
        "rights": (
            "textList",
            "oai_ddi:codeBook/stdyInfo/citation/prodStmt/copyright/text()",
        ),  # noqa
    },
    namespaces={
        "oai_ddi": "http://www.icpsr.umich.edu/DDI",
    },
)

# Note: maintainer_email is not part of Dublin Core
oai_dc_reader = MetadataReader(
    fields={
        "title": ("textList", "oai_dc:dc/dc:title/text()"),  # noqa
        "creator": ("textList", "oai_dc:dc/dc:creator/text()"),  # noqa
        "subject": ("textList", "oai_dc:dc/dc:subject/text()"),  # noqa
        "description": ("textList", "oai_dc:dc/dc:description/text()"),  # noqa
        "publisher": ("textList", "oai_dc:dc/dc:publisher/text()"),  # noqa
        "maintainer_email": (
            "textList",
            "oai_dc:dc/oai:maintainer_email/text()",
        ),  # noqa
        "contributor": ("textList", "oai_dc:dc/dc:contributor/text()"),  # noqa
        "date": ("textList", "oai_dc:dc/dc:date/text()"),  # noqa
        "type": ("textList", "oai_dc:dc/dc:type/text()"),  # noqa
        "format": ("textList", "oai_dc:dc/dc:format/text()"),  # noqa
        "identifier": ("textList", "oai_dc:dc/dc:identifier/text()"),  # noqa
        "source": ("textList", "oai_dc:dc/dc:source/text()"),  # noqa
        "language": ("textList", "oai_dc:dc/dc:language/text()"),  # noqa
        "relation": ("textList", "oai_dc:dc/dc:relation/text()"),  # noqa
        "coverage": ("textList", "oai_dc:dc/dc:coverage/text()"),  # noqa
        "rights": ("textList", "oai_dc:dc/dc:rights/text()"),  # noqa
    },
    namespaces={
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
    },
)

oai_datacite_reader = MetadataReader(
fields={
    'title':       ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:titles/oai_datacite:title/text()'), #needtolookagain
    'inchi':       ('textList','oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:titles/oai_datacite:title[@titleType="AlternativeTitle"]/text()'),
    'creator':     ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:creators/oai_datacite:creator/oai_datacite:creatorName/text()'),
    'subject':     ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:subjects/oai_datacite:subject/text()'),
    'description': ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:descriptions/oai_datacite:description/text()'), #needtolookagain
    'publisher':   ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:publisher/text()'),
    'contributor': ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:contributors/oai_datacite:contributor/oai_datacite:contributorName/text()'), #lookagain
    'date':        ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:dates/oai_datacite:date/text()'),
    #'type':        ('textList', '//resource/type/text()'),
    #'format':      ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/datacite:resource//text()'),
    'identifier':  ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:identifier/text()'),
    #'source':      ('textList', '//resource/source/text()'),
    'language':    ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:language/text()'),
    'relation':    ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:relatedIdentifiers/oai_datacite:relatedIdentifier/text()'),
    'relationType': ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:relatedIdentifiers/oai_datacite:relatedIdentifier/@relationType'),
    'relationIdType': ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:relatedIdentifiers/oai_datacite:relatedIdentifier/@relatedIdentifierType'),
    #'coverage':    ('textList', '//resource/coverage/text()'),
    'rights':      ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:rightsList/oai_datacite:rights/text()'),
    'publicationYear': ('textList', 'oai_datacite:oai_datacite/oai_datacite:payload/oai_datacite:resource/oai_datacite:publicationYear/text()')
    },
    namespaces={
        'oai' : 'http://www.openarchives.org/OAI/2.0/',
        'oai_datacite': 'http://schema.datacite.org/oai/oai-1.1/',
        'datacite': 'http://datacite.org/schema/kernel-4',
   }
)





