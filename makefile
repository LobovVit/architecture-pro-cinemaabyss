# --- Исходники / каталоги ---
C4_DIR         := docs/c4
ERD_DIR        := docs/erd
PUML_GLOBS     := $(C4_DIR)/*.puml $(ERD_DIR)/*.puml
IMG_DIR        := ../images

# ------------------------------------------------------------
# Основные цели
# ------------------------------------------------------------
.PHONY: all docs redoc diagrams openapi-docs asyncapi

all: docs
docs: diagrams

# ------------------------------------------------------------
# puml → png
# ------------------------------------------------------------
.PHONY: diagrams
diagrams:
	@mkdir -p $(IMG_DIR)
	@plantuml $(PUML_GLOBS) -o $(IMG_DIR)