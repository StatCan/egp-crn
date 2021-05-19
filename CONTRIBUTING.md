# Contributing

This document explains the process of contributing to the National Road Network (NRN) project.

The preferred mode of contribution is via GitHub issues. Code additions, modifications, and removals should be left to 
the NRN project members.

## Table of Contents

- [Statement of Public Availability](#statement-of-public-availability)
- [Opening Issues](#opening-issues)
- [Modifying Code](#modifying-code)
  * [Formatting](#formatting)
  * [Versioning](#versioning)

## Statement of Public Availability

The NRN is exposed as a publicly available repository to align with the growing number of initiatives and policies 
towards open data and transparency within the Government of Canada and, specifically, Statistics Canada. However, the 
NRN is still an official government product and, although the code is publicly available, not all components and 
details can be exposed, including:
- Original data sources (excluding those which are publicly available).
- Data providers and contact information.
- Historical products and scripts (including those created by Statistics Canada or any other NRN-affiliate).

## Opening Issues

Feedback is greatly appreciated whether it be an issue, idea, or general question. Follow these steps when opening a 
GitHub issue:
1. **Check pre-existing issues:** Browse opened and closed issues to check if your concern has already been addressed. 
   If this is the case, comment on the existing issue rather than opening a new issue.
2. **Opening an issue:** When opening a new issue, make use of the pre-existing labels and the associated template. Not 
   all sections of the template are required; to avoid redundancy, complete only those sections which you feel are 
   essential to your issue.
3. **Closing an issue:** Do not close an issue, even if you feel it is resolved or no longer relevant. This 
   responsibility should be left for NRN project members.

## Modifying Code

Code additions, modifications, and removals are allowed by non-NRN project members. However, given the nature of the 
NRN as an official government project, thorough code review should be expected prior to acceptance of any pull requests.
In addition, any contributed code will become property of the NRN and Statistics Canada.

### Formatting
The NRN project does not enforce any formatting standard, however, you should try to follow PEP 8 standards as much as 
possible.

### Versioning
The NRN project periodically updates the project dependencies, including the base Python version. Compatibility is seen 
as a non-issue since the NRN is intended to be run within its own conda environment.