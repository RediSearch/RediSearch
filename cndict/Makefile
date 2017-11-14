FRISO_INI 		:= friso.ini
FRISO_LEXDIR	:= lex

all: generated/libcndict_data.a

generated/libcndict_data.a: generated/cndict_data.o
	$(AR) rcs $@ $^

generated/cndict_data.c: bundle_friso.py
	python bundle_friso.py -i $(FRISO_INI) -d $(FRISO_LEXDIR) -o generated

clean:
	rm -rf generated
