Simple-Indexer
================

    Licence: The Open BSV License
    Maintainers: Roger Taylor, AustEcon
    Project Lead: Roger Taylor
    Homepage: https://electrumsv.io/


Overview
========
This server's sole purpose in life is to expose the future APIs that ElectrumSV needs in a RegTest
environment. It is not at all designed with scalability in mind. It is designed to be simple,
the code easy to read and understand and above all else is designed for correctness.

This will be used for functional testing of ElectrumSV but the secondary motive is that other
developers can test their own applications against this set of APIs. After all SPV applications
share a similar set of problems and so we hope this set of APIs can be broadly applicable to 
a wide range of domains.

This server will be included in the [ElectrumSV SDK](https://pypi.org/project/electrumsv-sdk) 
which aims to provide a first-class RegTest developer experience for running a local
(or cloud / pipelines-based) node as well as a number of other relevant services that many 
bitcoin applications depend on.


Running Simple Indexer
======================
On windows:

    pip install -r requirements.txt
    py server.py

On unix:

    pip3 install -r requirements.txt
    python3 server.py
