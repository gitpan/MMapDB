#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#if __GNUC__ >= 3
# define attribute(x) __attribute__(x)
# define expect(expr,value) __builtin_expect ((expr),(value))
# define INLINE static inline
#else
# define attribute(x)
# define expect(expr,value) (expr)
# define INLINE static
#endif

#define expect_false(expr) expect ((expr) != 0, 0)
#define expect_true(expr)  expect ((expr) != 0, 1)

typedef AV* MMapDB;

/* object AV layout -- must match @attributes in MMapDB.pm */
# define MMDB_FILENAME       0
# define MMDB_READONLY       1
# define MMDB_INTFMT         2
# define MMDB_DATA           3
# define MMDB_RWDATA         4
# define MMDB_INTSIZE        5
# define MMDB_STRINGFMT      6
# define MMDB_STRINGTBL      7
# define MMDB_MAINIDX        8
# define MMDB_IDIDX          9
# define MMDB_MAIN_INDEX    10
# define MMDB_ID_INDEX      11
# define MMDB_NEXTID        12
# define MMDB_IDMAP         13
# define MMDB_TMPFH         14
# define MMDB_TMPNAME       15
# define MMDB_STRINGFH      16
# define MMDB_STRINGMAP     17
# define MMDB_STRPOS        18
# define MMDB_LOCKFILE      19

# define identity(v) (v)

/* converts DB integer to host */
# define xI(type, cnv, var) ((type)cnv(var))

/* returns the pointer of a string given by its position in the string table */
/* pos: position in host byte order */
/* t: string table pointer */
# define xSp(type, cnv, t, pos) ((char*)(t)+sizeof(type)+(pos))

/* reads the length of a string given by its position from the string table */
/* pos: position in host byte order */
/* t: string table pointer */
# define xSl(type, cnv, t, pos) (cnv(((type*)((char*)(t)+(pos)))[0]))

INLINE int
cmp(const void* p1, int p1len, const void* p2, int p2len) {
  int rc=memcmp(p1, p2, p1len<p2len?p1len:p2len);
  return rc ? rc : p1len==p2len ? 0 : p1len<p2len ? -1 : 1;
}

# define GENFN(type, fmt, cnv)						\
  static void								\
  pushresult_##fmt(pTHX_ const void* _descr, SV** sp) {			\
    const type* descr=_descr;						\
    type npos;								\
    int i;								\
    npos=xI(type, cnv, *descr++);	/* position count */		\
    EXTEND(SP, npos);							\
    for( i=0; i<npos; i++ ) {						\
      mPUSHu(xI(type, cnv, descr[i]));					\
    }									\
    PUTBACK;								\
  }									\
									\
  static void*								\
  idx_lookup_##fmt(const char* k, int klen, const void* kidx,		\
		   const void* strtbl, UV dataend,			\
		   /* output */ int *isidx, UV* nextpos) {		\
    const type* idx=kidx;						\
    type high=xI(type, cnv, *idx++);					\
    type rlen=xI(type, cnv, *idx++);					\
    type low=0, cur, curoff;						\
    int rel;								\
    while( low<high ) {							\
      cur=(high+low)/2;							\
      curoff=xI(type, cnv, idx[rlen*cur]);				\
      rel=cmp(xSp(type, cnv, strtbl, curoff),				\
	      xSl(type, cnv, strtbl, curoff),				\
	      k, klen);							\
      if(rel<0) {							\
	low=cur+1;							\
      } else if(rel>0) {						\
	high=cur;							\
      } else {								\
	idx+=cur*rlen+1;	/* idx now points to the npos field */	\
	*isidx=(xI(type, cnv, idx[0])==1 &&				\
		xI(type, cnv, idx[1])>=dataend);			\
	*nextpos=xI(type, cnv, idx[1]);					\
	return (void*)(idx);						\
      }									\
    }									\
    return NULL;							\
  }									\
									\
  static UV								\
  ididx_lookup_##fmt(UV id, const void* kidx) {				\
    const type* idx=kidx;						\
    type high=xI(type, cnv, *idx++);					\
    type low=0, cur, curid;						\
    while( low<high ) {							\
      cur=(high+low)/2;							\
      curid=xI(type, cnv, idx[2*cur]);					\
      if(curid<id) {							\
	low=cur+1;							\
      } else if(curid>id) {						\
	high=cur;							\
      } else {								\
	return (UV)xI(type, cnv, idx[2*cur+1]);				\
      }									\
    }									\
    return (UV)-1;							\
  }									\
									\
  static AV*								\
  drec_##fmt(pTHX_ const void* _rec, const void* _strtbl) {		\
    const type* rec=_rec;						\
    const type* lenp;							\
    const char* strtbl=_strtbl;						\
    type id, nkeys, i;							\
    AV* av=newAV();							\
    AV* res=newAV();							\
    SV *sv;								\
									\
    rec++;      /* skip valid flag */					\
    id=xI(type, cnv, *rec++);						\
    nkeys=xI(type, cnv, *rec++);					\
    av_extend(av, nkeys);						\
									\
    for(i=0; i<nkeys; i++) {						\
      lenp=(const type*)(strtbl+xI(type, cnv, *rec++));			\
      sv=newSV(0);							\
      SvUPGRADE(sv, SVt_PV);						\
      SvPOK_only(sv);							\
      SvPV_set(sv, (char*)(lenp+1));					\
      SvLEN_set(sv, 0);							\
      SvCUR_set(sv, xI(type, cnv, *lenp));				\
      SvREADONLY_on(sv);						\
      av_push(av, sv);							\
    }									\
    									\
    av_extend(res, 4);							\
    av_push(res, newRV_noinc((SV*)av));					\
									\
    for( i=0; i<2; i++ ) {						\
      lenp=(const type*)(strtbl+xI(type, cnv, *rec++));			\
      sv=newSV(0);							\
      SvUPGRADE(sv, SVt_PV);						\
      SvPOK_only(sv);							\
      SvPV_set(sv, (char*)(lenp+1));					\
      SvLEN_set(sv, 0);							\
      SvCUR_set(sv, xI(type, cnv, *lenp));				\
      SvREADONLY_on(sv);						\
      av_push(res, sv);							\
    }									\
    av_push(res, newSVuv(id));						\
    return res;								\
  }

GENFN(U32,     L, identity)
GENFN(UV,      J, identity)
GENFN(U32,     N, ntohl)
# ifdef HAS_QUAD
GENFN(U64TYPE, Q, identity)
# endif

typedef void* (*idx_lookup)(const char *k, int klen, const void *kidx,
			    const void *strtbl, UV dataend,
			    /* output params */
			    int* isidx, UV* nextpos);
typedef void (*pushresult)(pTHX_ const void* _descr, SV** sp);
typedef AV* (*drec)(pTHX_ const void* _rec, const void* _strtbl);
typedef UV (*ididx_lookup)(UV id, const void *kidx);

# define USEFN(fmt) {idx_lookup_##fmt, pushresult_##fmt, ididx_lookup_##fmt, \
      drec_##fmt}
# define NULLFN {0,0,0,0}

struct {
  idx_lookup idx;
  pushresult pres;
  ididx_lookup ididx;
  drec drec;
} lookup[]={
# ifdef EBCDIC
  USEFN(L),
  USEFN(N),
#   ifdef HAS_QUAD
  USEFN(Q),
#   else
  NULLFN,
#   endif
  USEFN(J),
# else	/* ASCII */
#   ifdef HAS_QUAD
  USEFN(Q),
#   else
  NULLFN,
#   endif
  USEFN(J),
  USEFN(L),
  USEFN(N),
# endif
};

# ifdef EBCDIC
/* XXX: untested due to lack of hardware */
#   define L(c, m) (*((lookup[(((c)-3)>>1) & 3]).m))
# else
#   define L(c, m) (*((lookup[((c)>>1) & 3]).m))
# endif


MODULE = MMapDB		PACKAGE = MMapDB		

PROTOTYPES: DISABLED

void
index_lookup(I, ...)
    MMapDB I;
  PPCODE:
    if( expect_true(items>1) ) {
      UV pos=SvUV(ST(1));
      STRLEN keylen;
      char *datap, *intfmt, *keyp;
      SV **svp=av_fetch(I, MMDB_DATA, 0);
      void *strtbl, *found=0;
      UV dataend;
      int i, isidx=1;

      if( expect_false(!(svp && SvROK(*svp))) ) goto END;
      datap=SvPV_nolen(SvRV(*svp));

      intfmt=SvPV_nolen(*av_fetch(I, MMDB_INTFMT, 0));
      strtbl=datap+SvUV(*av_fetch(I, MMDB_STRINGTBL, 0));
      dataend=SvUV(*av_fetch(I, MMDB_MAINIDX, 0));

      for(i=2; i<items && isidx; i++) {
	keyp=SvPV(ST(i), keylen);
	found=L(intfmt[0],idx)(keyp, keylen, datap+pos, strtbl, dataend,
			       &isidx, &pos);
	/* don't know what to expect here */
	if(!found) goto END;
      }

      /* but if we are here it is probably a hit */
      if( expect_true(found && i==items) ) {
	L(intfmt[0],pres)(aTHX_ found, sp);
	/* pres() calls PUTPACK. So, we must return here */
	return;
      }
    END:
      PUTBACK;
    }

void
id_index_lookup(I, id)
    MMapDB I;
    UV id;
  PPCODE:
    {
      char *datap, *intfmt;
      UV pos;
      SV **svp=av_fetch(I, MMDB_DATA, 0);

      if( expect_false(!(svp && SvROK(*svp))) ) return;
      datap=SvPV_nolen(SvRV(*svp));

      intfmt=SvPV_nolen(*av_fetch(I, MMDB_INTFMT, 0));
      pos=SvUV(*av_fetch(I, MMDB_IDIDX, 0));

      pos=L(intfmt[0],ididx)(id, datap+pos);

      if( pos!=(UV)-1 ) {
	/* EXTEND(SP,1); # not necessary there is already room for 2 items */
	PUSHs(sv_2mortal(newSVuv(pos)));
      }
    }

void
data_record(I, ...)
    MMapDB I;
  PPCODE:
    if( items>1 ) {
      UV pos=SvUV(ST(1));
      char *datap, *intfmt;
      UV dataend, stroff;
      SV **svp=av_fetch(I, MMDB_DATA, 0);
      AV* av;

      if( expect_false(!(svp && SvROK(*svp))) ) return;
      datap=SvPV_nolen(SvRV(*svp));

      dataend=SvUV(*av_fetch(I, MMDB_MAINIDX, 0));

      if( expect_true(pos<dataend) ) {
	intfmt=SvPV_nolen(*av_fetch(I, MMDB_INTFMT, 0));
	stroff=SvUV(*av_fetch(I, MMDB_STRINGTBL, 0));

	av=L(intfmt[0],drec)(aTHX_ datap+pos, datap+stroff);
	PUSHs(sv_2mortal(newRV_noinc((SV*)av)));
      }
    }
