# Include platform dependent makefiles
ifeq ($(OS),Windows_NT)
include Makefile.nt
else
include Makefile.unix
endif

PREFIX:=bin/

#############################################################################
# Default target
all: $(PREFIX)rdf3xdump$(EXEEXT) $(PREFIX)rdf3xload$(EXEEXT) $(PREFIX)rdf3xquery$(EXEEXT)

#############################################################################
# Collect all sources

include cts/LocalMakefile
include infra/LocalMakefile
include makeutil/LocalMakefile
include rts/LocalMakefile

ifeq ($(LINEEDITOR),1)
src_lineeditor:=lineeditor/LineInput.cpp lineeditor/LineEditor.cpp lineeditor/Terminal.cpp lineeditor/Display.cpp lineeditor/Buffer.cpp
endif

include tools/LocalMakefile


source:=$(src_cts) $(src_infra) $(src_rts) $(src_tools) $(src_lineeditor)

#############################################################################
# Dependencies

generatedependencies=$(call nativefile,$(PREFIX)makeutil/getdep) -o$(basename $@).d $(IFLAGS) $< $(basename $@)$(OBJEXT) $(genheaders) $(GENERATED-$<)

ifneq ($(IGNORE_DEPENDENCIES),1)
-include $(addprefix $(PREFIX),$(source:.cpp=.d)) $(addsuffix .d,$(basename $(wildcard $(generatedsource))))
endif

#############################################################################
# Compiling

compile=$(CXX) -c $(TARGET)$(call nativefile,$@) $(CXXFLAGS) $(IFLAGS) $(call nativefile,$<)

$(PREFIX)%$(OBJEXT): %.cpp $(PREFIX)makeutil/getdep$(EXEEXT)
	$(checkdir)
	$(generatedependencies)
	$(compile)

#############################################################################
# Cleanup

clean:
	find bin -name '*.d' -delete -o -name '*.o' -delete -o '(' -perm /u=x '!' -type d ')' -delete

#############################################################################
# Executable

$(PREFIX)query: $(addprefix $(PREFIX),$(source:.cpp=$(OBJEXT)))

