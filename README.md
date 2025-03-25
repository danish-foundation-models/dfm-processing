<div id="top">

<!-- HEADER STYLE: CLASSIC -->
<div align="center">

<img src="readmeai/assets/logos/purple.svg" width="30%" style="position: relative; top: 0; right: 0;" alt="Project Logo"/>

# DFM-PROCESSING

<em>Effortlessly Deduplicate and Process Data at Scale</em>

<!-- BADGES -->
<img src="https://img.shields.io/github/license/danish-foundation-models/dfm-processing?style=default&logo=opensourceinitiative&logoColor=white&color=0080ff" alt="license">
<img src="https://img.shields.io/github/last-commit/danish-foundation-models/dfm-processing?style=default&logo=git&logoColor=white&color=0080ff" alt="last-commit">
<img src="https://img.shields.io/github/languages/top/danish-foundation-models/dfm-processing?style=default&color=0080ff" alt="repo-top-language">
<img src="https://img.shields.io/github/languages/count/danish-foundation-models/dfm-processing?style=default&color=0080ff" alt="repo-language-count">

<!-- default option, no dependency badges. -->


<!-- default option, no dependency badges. -->

</div>
<br>

---

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
    - [Usage](#cli-usage)
- [More information](#more-information)
- [Wish to contribute?](#wish-to-contribute)

---

## Overview

Danish Foundation Models is a collaborative project for training foundational Danish language model. Which seeks to:

- Develop and maintain **state-of-the-art models** for Danish,
- which are **well-validated** across a wide range of tasks.
- Furthermore, we wish to **ensure good documentation**, which allows users to assess the model for their use-case critically
- **Open-source**, both model and source code

*Note*: This repository is intended for the data processing of DFM.


---

## Project Structure

```sh
└── dfm-processing/
    ├── .github
    │   └── workflows
    ├── LICENSE
    ├── README.md
    ├── config
    │   └── example.yaml
    ├── pyproject.toml
    ├── src
    │   └── dfm_processing
    ├── tests
    │   ├── cli
    │   ├── data_pipeline
    │   └── document_processing
    └── uv.lock
```

---

## Getting Started

### Prerequisites

This project requires the following dependencies:

- **Programming Language:** Python
- **Package Manager:** Uv

### Installation

Build dfm-processing from the source and intsall dependencies:

1. **Clone the repository:**

    ```sh
    ❯ git clone https://github.com/danish-foundation-models/dfm-processing
    ```

2. **Navigate to the project directory:**

    ```sh
    ❯ cd dfm-processing
    ```

3. **Install the dependencies:**
	<!-- SHIELDS BADGE CURRENTLY DISABLED -->
	<!-- [![uv][uv-shield]][uv-link] -->
	<!-- REFERENCE LINKS -->
	<!-- [uv-shield]: https://img.shields.io/badge/uv-DE5FE9.svg?style=for-the-badge&logo=uv&logoColor=white -->
	<!-- [uv-link]: https://docs.astral.sh/uv/ -->
	**Using [uv](https://docs.astral.sh/uv/):**

	```sh
	❯ uv sync --all-extras
	```

### CLI Usage

The CLI is divided into two sections, "document" and "pipeline". Each section contains specific commands for different tasks.

#### Document Processing (`document`)

1. **Process Directory:**
   - **Purpose:** Extract text data from various file types in a directory.
   - **Usage:**
     ```bash
     uv run dfm-processing document process-directory path_to_dir output_dir dataset_name
     ```
   - **Example:**
     ```bash
     uv run dfm-processing document process-directory ./data ./output my_dataset
     ```

2. **Process Web Crawl:**
   - **Purpose:** Extract text data from a web crawl.
   - **Usage:**
     ```bash
     uv run dfm-processing document process-web-crawl crawl_log output_dir crawled_data dataset_name
     ```
   - **Example:**
     ```bash
     uv run dfm-processing document process-web-crawl example.com.log ./output ./crawled_data/ example.com
     ```

### Data Pipeline (`pipeline`)

1. **Filter:**
   - **Purpose:** Run a filtering pipeline on a dataset to filter out "poor" quality data.
   - **Usage:**
     ```bash
     uv run dfm-processing pipeline filter yaml_config
     ```
   - **Example:**
     ```bash
     uv run dfm-processing pipeline filter ./config/example.yaml
     ```

2. **Sentence Deduplication (`sent_dedup`):**
   - **Purpose:** Perform sentence deduplication on a given dataset.
   - **Usage:**
     ```bash
     uv run dfm-processing pipeline sent_dedup yaml_config
     ```
   - **Example:**
     ```bash
     uv run dfm-processing pipeline sent_dedup ./config/example.yaml
     ```

3. **MinHash Deduplication (`minhash-dedup`):**
   - **Purpose:** Perform MinHash Deduplication on a given dataset.
   - **Usage:**
     ```bash
     uv run dfm-processing pipeline minhash-dedup yaml_config
     ```
   - **Example:**
     ```bash
     uv run dfm-processing pipeline minhash-dedup ./config/example.yaml
     ```

---

## More information:
For more information please check out the following links:

|                                                                                                         |                                                                                                         |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 📑 [**About**](https://foundationmodels.dk/)              | A overview of the DFM project                                                                           |
| [**Research Paper**](https://arxiv.org/abs/2311.07264)                                                  | An paper introducing DFM and its rationale                                                              |
| 🚀 [**Models**](https://www.foundationmodels.dk/models/) | A overview of current models available through the DFM project                                          |
| 💽 [**Datasets**](https://huggingface.co/datasets/danish-foundation-models/danish-dynaword)       | Includes datasheets about the datasets which includes preprocessing, reason for constructions and more. |



## Wish to contribute?
DFM is considered a collaborative project for training and maintaining Danish Language models. If you wish to contribute don't hesitate to reach out using one of the following channels:

|                                                                                                                      |                                                               |
| -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| 🗣 [**DDSC Slack**](https://join.slack.com/t/danskdatascie-o8m9638/shared_invite/zt-1jh2dwmj4-D_mjywfXERvVP75n9O0ykg) | Join the discussion in the "danish-foundation-models"-channel |
| 💬 [**GitHub Discussion**](https://github.com/danish-foundation-models/dfm-processing/discussions)   | Ask questions or start a discussion                           |
| 🚨 [**GitHub Issues**](https://github.com/danish-foundation-models/dfm-processing/issues)            | Notices a bug in the code? Please create an issue             |

You can contribute both:

-  Developer time, the lifeblood of any open-source project
-  Pre-training datasets you wish to include in the model training
-  Validation tasks can even be private benchmarks where you only wish to share the performance metrics.
- And probably in many other ways

<div align="right">

[![][back-to-top]](#top)

</div>


[back-to-top]: https://img.shields.io/badge/-BACK_TO_TOP-151515?style=flat-square


---
