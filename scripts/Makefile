# build:   make
# install: sudo make install          (or: make install PREFIX=$HOME/.local)
# remove:  sudo make uninstall

CXX      ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -std=c++17
PREFIX   ?= /usr/local

all: get_waf

get_waf: get_waf.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

install: get_waf
	install -D -m 755 get_waf $(DESTDIR)$(PREFIX)/bin/get_waf

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/get_waf

clean:
	rm -f get_waf

.PHONY: all install uninstall clean
