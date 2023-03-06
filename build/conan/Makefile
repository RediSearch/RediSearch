
all: $(BINROOT)/conan/CMakePresets.json

$(BINROOT)/conan/CMakePresets.json: conanfile.txt
	@mkdir -p $(BINROOT)/conan
	@conan install . --output-folder=$(BINROOT)/conan --build=missing
