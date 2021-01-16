# openclean Documentation

This directory contains openclean's documentation.
It is writen using `reStructuredText` format and is compiled to multiple media formats using Sphinx.
The final documentation is automatically compiled and published after every commit to http://openclean.readthedocs.io/en/latest/

### Requirements

This documentation uses the following extra Sphinx packages:
 * sphinx
 * sphinx_rtd_theme
 * sphinxcontrib.apidoc
 * jupyter_sphinx.execute

If you use `conda`, these packages can be installed using:

```
conda install sphinx
conda install sphinx_rtd_theme
conda install sphinxcontrib_apidoc
conda install jupyter_sphinx_execute
```
or using `pip`:

```
pip install sphinx sphinx-rtd-theme sphinxcontrib-apidoc jupyter-sphinx
```

### Editing the Documentation

You can edit any `.rst` file, and then use the make to command to compile the docs:

    make html

Finally, open the generated HTML file using your web browser. The entry page of
the documentation can be found at: ``_build/html/index.html``.
