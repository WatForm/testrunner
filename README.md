# Simple Evaluation Test Harness

We commonly have to run a command at the CLI over a variety of inputs and options.  We have to time the running of this command and tally its results in a CSV file.

This repo contains some simple scripts to run commands at the CLI with options for performance evaluations.  The top-level script is testrunner.py .  Helper functions are found in util.py .

## Simple Example

The testrunner code is usually used from outside its directory.  Please see its sibling github repo:
`git clone https://github.com/WatForm/example_using_testrunner.git`
for examples and documentation on how to use the testrunner code.

It is unlikely that you should have to modify the scripts in this repor to run your own evaluation.  But if you do modify these scripts then perhaps, the testrunner repo should be updated.

## Acknowledgements

These scripts were mainly written by Owen Zila with modifications by Nancy Day.
