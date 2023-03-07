
all: $(BINROOT)/conan/CMakePresets.json

$(BINROOT)/conan/CMakePresets.json: conanfile.txt
	@if [[ ! -e .conan2/profiles/default ]]; then conan profile detect --force; fi
	@mkdir -p $(BINROOT)/conan
	@conan install . --output-folder=$(BINROOT)/conan --build=missing
