
doc1_content = r'''{
    "name": "wonderbar",
    "category": ["mathematics and computer science", "logic", "programming", "database"],
    "books": [ 
        {
            "name": "Structure and Interpretation of Computer Programs",
            "authors": [
                "Harold Abelson", "Gerald Jay Sussman", "Julie Sussman"
            ]
        },
        {
            "name": "The Art of Computer Programming",
            "authors": [
                "Donald Knuth"
            ]
        },
        {
            "name": "Introduction to Algorithms",
            "authors": [
                "Thomas H. Cormen", "Charles E. Leiserson", "Ronald L. Rivest", "Clifford Stein"
            ]
        },
        {
            "name": "Classical Mathematical Logic: The Semantic Foundations of Logic",
            "authors": [
                "Richard L. Epstein"
            ]
        },
        {
            "name": "Design Patterns: Elements of Reusable Object-Oriented Software",
            "authors": [
                "Erich Gamma", "Richard Helm", "Ralph Johnson", "John Vlissides"
            ]
        },
        {
            "name": "Redis Microservices for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Redis 4.x Cookbook",
            "authors": [
                "Pengcheng Huang", "Zuofei Wang"
            ]
        }
    ]}
'''

doc2_content = r'''{
    "name": "foo",
    "category": ["database", "high performance"],
    "books": [ 
        {
            "name": "Redis for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Redis Microservices for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Systems Performance - Enterprise and the Cloud",
            "authors": [
                "Brendan Gregg"
            ]
        }
    ]}
'''

doc3_content = r'''{
    "name": "bar",
    "category": ["performance", "cloud"],
    "books": [ 
        {
            "name": "Redis for Dummies",
            "authors": [
                "Redis Ltd."
            ]
        },
        {
            "name": "Designing Data-Intensive Applications",
            "authors": [
                "Martin Kleppmann"
            ]
        },
        {
            "name": "Kubernetes: Up and Running",
            "authors": [
                "Kelsey Hightower", "Brendan Burns", "Joe Beda"
            ]
        }
    ]}
'''

doc4_content = r'''{
    "name": "rebar",
    "category": ["general"],
    "books": [ 
        {
            "name": "Redis for Dummies",
            "authors": "Redis Ltd."
        }
    ]}
'''

doc_non_text_content = r'''{
    "attr1": ["first", "second", null, "third", null , "null", null],
    "attr2": "third",
    "attr3": [null, null],
    "attr4": [],
    "attr5": null,
    "attr6": ["first", "second", null, "third", null, 2.04 ],
    "attr7": ["first", "second", null, "third", null, false ],
    "attr8": ["first", "second", null, "third", null, {"obj": "ection"} ],
    "attr9": ["first", "second", null, "third", null, ["recursi", "on"] ],
    "attr10": ["first", "second", null, "third", null, ["recursi", 50071] ]
}
'''
