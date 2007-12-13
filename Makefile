# Include platform dependent makefiles
ifeq ($(OS),Windows_NT)
include Makefile.nt
else
include Makefile.unix
endif

PREFIX:=bin/

#############################################################################
# Default target
all: $(PREFIX)buildrdfstore$(EXEEXT)

#############################################################################
# Collect all sources

include cts/LocalMakefile
include infra/LocalMakefile
include makeutil/LocalMakefile
include rts/LocalMakefile
include tools/LocalMakefile

source:=$(src_cts) $(src_infra) $(src_rts) $(src_tools)

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
# Executable

$(PREFIX)query: $(addprefix $(PREFIX),$(source:.cpp=$(OBJEXT)))
