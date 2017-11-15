FRISO_INI 		:= friso.ini
FRISO_LEXDIR	:= lex
PYTHON			:= python

cndict_data.c: bundle_friso.py
	$(PYTHON) bundle_friso.py -i $(FRISO_INI) -d $(FRISO_LEXDIR) -o .

clean:
	rm -rf cndict_data.c
