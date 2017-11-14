all: generated/libcndict_data.a

generated/libcndict_data.a: generated/cndict_data.o
	$(AR) rcs $@ $^

generated/cndict_data.c: bundle_friso.py
	python bundle_friso.py -i friso.ini -d lex -o generated

clean:
	rm -rf generated
