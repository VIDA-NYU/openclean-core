.. _contribute-ref:

Contributing
============
openclean is a community effort and is continuously growing. All contributions, bug reports, bug fixes,
documentation improvements, enhancements, and ideas are welcome.


Code
----
We encourage developers to contribute!

If you are brand new to openclean or open-source development, we recommend going through the GitHub
`“issues” <https://github.com/VIDA-NYU/openclean-core/issues>`_ tab to find issues that interest you.
When you start working on an issue, it’s a good idea to assign the issue to yourself, so nobody else
duplicates the work on it. However, GitHub restricts assigning issues to maintainers of the project only.
Therefore, contributors are requested to add a comment letting others know they are working on an issue.

If for whatever reason you are not able to continue working with the issue, please try to un-assign it (or comment there),
so other people know it’s available again. If you want to work on one that is assigned, feel free to kindly ask the
current assignee if you can take it (please allow at least a week of inactivity before considering work in the issue
discontinued).


Test Coverage
-------------
High-quality unit testing is the mainstay of the openclean development process. For this purpose, we use
the pytest package. The tests are functions appropriately named, located in tests subdirectories, that check the
validity of the operators and the different options of the code.

Running pytest in a folder will run all the tests of the corresponding subpackages. Please ensure, all submitted pull
requests have unit tests accompanying the source code wherever applicable. We expect code coverage of new features
to be at least around 90%.


Bug report or feature request
-----------------------------
We use GitHub issues to track all bugs and feature requests; feel free to open an issue if you have found a bug or wish
to see a feature implemented.

It is recommended to check that your issue complies with the following rules before submitting:

* Verify that your issue is not being currently addressed by other issues or pull requests.
* If you are submitting a bug report, we strongly encourage you to follow the guidelines in `How to make a good bug report <https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports>`_ with a `reproducible example <https://stackoverflow.com/help/minimal-reproducible-example>`_ (if applicable). The ideal bug report contains a short reproducible code snippet, this way anyone can try to reproduce the bug easily and provide good feedback. If your snippet is longer than around 50 lines, please link to a gist or a github repo. If not feasible to include a reproducible snippet, please be specific about what operations and/or functions are involved and the shape of the data.
* If an exception is raised, please provide the full traceback. Please include your operating system type and version number, as well as your Python, and openclean versions and any other information you deem useful.


Documentation
-------------
As developers we understand that sometimes, there are sections of documentations that are worse off explaining stuff
to users because they've been written by experts. Therefore, we encourage you to help us improve the documentation
If something in the docs doesn’t make sense to you, updating the relevant section after you figure it out is a great
way to ensure it will help others.

The openclean documentation has been written in reStructuredText and built using sphinx. The complete list of
requirements along with how to work with the documentation is described `here <https://github.com/VIDA-NYU/openclean-core/tree/master/docs>`_.
