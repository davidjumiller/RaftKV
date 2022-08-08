# RaftKV - A distributed key-value data store, created for CPSC 416

In the project, we present a distributed key value store which uses Raft as backbone support for providing consensus and failure handling. The application will handle common failures like network partition or node failure, and only fail to respond when the majority of all working servers fail. It should also be able to handle new server join or a scenario of rejoining when previously failed servers are back online.

[Project Proposal](https://docs.google.com/document/d/1F1kRdzZonPuTNJB91N2Sqg59TD_I6zTZ37phJqRWuk0/edit?usp=sharing)

[Project Report](https://docs.google.com/document/d/1qoafSMMeXFOkbtHBCqcp3mrYMhMvkhB_bBF4Q-alhxo/edit?usp=sharing)

The pdf version of proposal and report are under report/. It might not be the most up-to-date one so the google docs is more preferable.

There are also some shiviz logs that can used for visulization in the docs/

We would like to give credit to the various resources online about raft implmentations and distributed kv systems:
1. [Raft](https://raft.github.io/)
2. [Raft Extended Paper](https://raft.github.io/raft.pdf)
3. [UIUC Slides](https://raft.github.io/slides/uiuc2016.pdf)
4. [MIT 6.824 Student Repo](https://github.com/WenbinZhu/mit-6.824-labs): We didn't copy the code but only read the repo to get a sense about how raft should be implemented. Our raft is also different from theirs in order to integrate into our server setting
5. [MIT 6.824 Spec](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
6. [hashicorp/raft](https://github.com/hashicorp/raft)
7. [TiKV](https://github.com/pingcap/tikv)

