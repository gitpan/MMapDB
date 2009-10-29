package MMapDB;

use 5.008008;
use strict;
use warnings;
no warnings qw/uninitialized/;

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# keep this in mind
use integer;
use bytes;
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

use Fcntl qw/:seek :flock/;
use File::Spec;
use File::Map qw/map_handle/;
use Exporter qw/import/;

{				# limit visibility of "our"/"my" variables
  our $VERSION = '0.04';
  our %EXPORT_TAGS=
    (
     error=>[qw/E_READONLY E_TWICE E_TRANSACTION E_FULL E_DUPLICATE
		E_OPEN E_READ E_WRITE E_CLOSE E_RENAME E_TRUNCATE E_LOCK/],
    );
  my %seen;
  undef @seen{map {@$_} values %EXPORT_TAGS};
  our @EXPORT_OK=keys %seen;
  $EXPORT_TAGS{all}=\@EXPORT_OK;

  require XSLoader;
  XSLoader::load('MMapDB', $VERSION);

  my @attributes;
  BEGIN {
    # define attributes and implement accessor methods
    # !! keep in sync with MMapDB.xs !!
    @attributes=(qw/filename readonly intfmt _data _rwdata _intsize _stringfmt
		    _stringtbl mainidx _ididx main_index id_index/,
		 # the next fields are valid only within a transaction
		 qw/_nextid _idmap _tmpfh _tmpname _stringfh _stringmap
		    _strpos lockfile/);
    for( my $i=0; $i<@attributes; $i++ ) {
      my $method_num=$i;
      ## no critic
      no strict 'refs';
      *{__PACKAGE__.'::'.$attributes[$method_num]}=
	sub : lvalue {$_[0]->[$method_num]};
      ## use critic
    }
  }
}

BEGIN {
  use constant {
    MAGICNUMBER   => 'MMDB',
    FORMATVERSION => 0,		# magic number position (in bytes)
    INTFMT        => 4,		# INTFMT byte position (in bytes)
    BASEOFFSET    => 8,
    MAINIDX       => 0,		# (in words (units of _intsize bytes))
    IDIDX         => 1,		# (in words)
    NEXTID        => 2,		# (in words)
    STRINGTBL     => 3,		# (in words)
    DATASTART     => 4,		# (in words)

    E_READONLY    => \'database is read-only',
    E_TWICE       => \'can\'t insert the same ID twice',
    E_TRANSACTION => \'there is already an active transaction',
    E_FULL        => \'can\'t allocate ID',
    E_DUPLICATE   => \'data records cannot be mixed up with subindices',

    E_OPEN        => \'can\'t open file',
    E_READ        => \'can\'t read from file',
    E_WRITE       => \'can\'t write to file',
    E_CLOSE       => \'file could not be closed',
    E_RENAME      => \'can\'t rename file',
    E_SEEK        => \'can\'t move file pointer',
    E_TRUNCATE    => \'can\'t truncate file',
    E_LOCK        => \'can\'t (un)lock lockfile',
  };
}

sub set_intfmt {
  my ($I, $fmt)=@_;

  $fmt='N' unless $fmt;

  my %allowed; undef @allowed{qw/L N J Q/};
  return unless exists $allowed{$fmt};

  $I->intfmt=$fmt;
  $I->_intsize=length pack($fmt, 0);
  $I->_stringfmt=$I->intfmt.'/a* x!'.$I->_intsize;

  return 1;
}

sub new {
  my ($parent, @param)=@_;
  my $I;

  if (ref $parent) {
    $I=bless [@$parent]=>ref($parent);
    for my $k (qw/_nextid _idmap _tmpfh _tmpname _stringfh _stringmap
		  _strpos/) {
      undef $I->$k;
    }
  } else {
    $I=bless []=>$parent;
    $I->set_intfmt('N');
    $I->_rwdata=do {my $dummy=' '; \$dummy};
  }

  while( my ($k, $v)=splice @param, 0, 2 ) {
    if( do {no strict 'refs'; defined &$k} ) {
      $I->$k=$v;
    }
  }
  $I->set_intfmt($I->intfmt) unless $I->intfmt eq 'N';

  return $I;
}

sub is_valid {
  my ($I)=@_;

  return unless $I->_data;
  # the INTFMT field serves 2 ways:
  #  1) it specifies the used integer format
  #  2) it works as VALID flag. commit() write a NULL byte here
  #     to mark the old file as invalid.
  #     we must reconnect if our cached fmt does not match.
  return substr( ${$I->_data}, INTFMT, 1 ) eq $I->intfmt;
}

sub start {
  my ($I)=@_;

  $I->_e(E_TRANSACTION) if defined $I->_tmpfh;

  my $retry=5;
  RETRY: {
      return unless $retry--;
      if( $I->_data ) {
	$I->stop unless( substr( ${$I->_data}, INTFMT, 1 ) eq $I->intfmt );
      }

      unless( $I->_data ) {
	my ($dummy, $rwdummy, $fmt);
	open my $fh, ($I->readonly ? '<' : '+<'), $I->filename or return;

	# Map the main data always read-only. If we are in writable mode
	# map only the header page again writable.
	eval {
	  map_handle $dummy, $fh, '<';
	  map_handle $rwdummy, $fh, '+<', INTFMT, 1 unless $I->readonly;
	};
	close $fh;
	return if $@;		# perhaps throw something here
	return unless length $dummy;

	# check magic number
	return unless substr($dummy, FORMATVERSION, 4) eq MAGICNUMBER;

	# read integer format
	$fmt=unpack 'x4a', $dummy;
	redo RETRY if( $fmt eq "\0" );
	return unless $I->set_intfmt($fmt);

	$I->_data=\$dummy;		# now mapped
	$I->_rwdata=\$rwdummy;

	# read main index position
	$I->mainidx=unpack('x'.(BASEOFFSET+MAINIDX*$I->_intsize).$I->intfmt,
			   ${$I->_data});
	$I->_ididx=unpack('x'.(BASEOFFSET+IDIDX*$I->_intsize).$I->intfmt,
			  ${$I->_data});
	$I->_stringtbl=unpack('x'.(BASEOFFSET+STRINGTBL*$I->_intsize).
			      $I->intfmt, ${$I->_data});

	tie %{$I->main_index=+{}}, 'MMapDB::Index', $I, $I->mainidx;
	tie %{$I->id_index=+{}}, 'MMapDB::IDIndex', $I;
      }
    }

  return $I;
}

sub stop {
  my ($I)=@_;

  $I->_e(E_TRANSACTION) if defined $I->_tmpfh;

  for my $k (qw/_data _rwdata _stringtbl mainidx _ididx/) {
    undef $I->$k;
  }
  $I->_rwdata=do {my $dummy=' '; \$dummy};
  return $I;
}

sub index_iterator {
  my ($I, $pos)=@_;

  my $data=$I->_data;
  return sub {} unless $data;
  my $fmt=$I->intfmt;
  my $isz=$I->_intsize;
  my ($nrecords, $recordlen)=unpack 'x'.$pos.$fmt.'2', $$data;
  $recordlen*=$isz;
  my ($cur, $end)=($pos+2*$isz,
		   $pos+2*$isz+$nrecords*$recordlen);
  my $stroff=$I->_stringtbl;
  my $sfmt=$I->_stringfmt;

  return sub {
    return if $cur>=$end;
    my ($key, $npos)=unpack 'x'.$cur.$fmt.'2', $$data;
    my @list=unpack 'x'.($cur+2*$isz).$fmt.$npos, $$data;
    $cur+=$recordlen;
    return (unpack('x'.($stroff+$key).$sfmt, $$data), @list);
  };
}

sub id_index_iterator {
  my ($I)=@_;

  my $data=$I->_data;
  return sub {} unless $data;
  my $pos=$I->_ididx;
  my ($nrecords)=unpack 'x'.$pos.$I->intfmt, $$data;
  my $recordlen=2*$I->_intsize;
  my ($cur, $end)=($pos+$I->_intsize,
		   $pos+$I->_intsize+$nrecords*$recordlen);
  my $fmt=$I->intfmt.'2';

  return sub {
    return if $cur>=$end;
    my @l=unpack 'x'.$cur.$fmt, $$data;
    $cur+=$recordlen;
    return @l;
  };
}

sub _e {$_[0]->_rollback; die $_[1]}
sub _ct {$_[0]->_tmpfh or die E_TRANSACTION}

sub begin {
  my ($I)=@_;

  $I->_e(E_TRANSACTION) if defined $I->_tmpfh;

  die E_READONLY if $I->readonly;

  if( defined $I->lockfile ) {
    # open lockfile
    unless( ref $I->lockfile ) {
      open my $fh, '>', $I->lockfile or die E_OPEN;
      $I->lockfile=$fh;
    }
    flock $I->lockfile, LOCK_EX or die E_LOCK;
  }

  {
    # open tmpfile
    open my $fh, '+>', $I->_tmpname=$I->filename.'.'.$$ or die E_OPEN;
    $I->_tmpfh=$fh;		# this starts the transaction

    syswrite $fh, pack('a4a', MAGICNUMBER, $I->intfmt) or $I->_e(E_WRITE);
    sysseek $fh, BASEOFFSET+DATASTART*$I->_intsize, SEEK_SET or $I->_e(E_SEEK);
    truncate $fh, BASEOFFSET+DATASTART*$I->_intsize or $I->_e(E_TRUNCATE);;
  }

  {
    # open stringtbl tmpfile
    open my $fh, '+>', $I->_tmpname.'.strings' or die E_OPEN;
    $I->_stringfh=$fh;
    $I->_stringmap=\(my $dummy='');
  }

  # and copy every *valid* entry from the old file
  # create _idmap on the way
  $I->_idmap={};
  $I->_strpos=[];
  for( my $it=$I->iterator; my ($pos)=$it->(); ) {
    $I->insert($I->data_record($pos));
  }
  if( $I->_data ) {
    $I->_nextid=unpack('x'.(BASEOFFSET+NEXTID*$I->_intsize).$I->intfmt,
		       ${$I->_data});
  } else {
    $I->_nextid=1;
  }
}

# The interator() below hops over the mmapped area. This one works on the file.
# It can be used only within a begin/commit cycle.
sub _fiterator {
  my ($I, $end)=@_;

  my $fh=$I->_tmpfh;
  my $pos=BASEOFFSET+$I->_intsize*DATASTART;

  return sub {
  LOOP: {
      return if $pos>=$end;
      my $elpos=$pos;

      sysseek $fh, $pos, SEEK_SET or $I->_e(E_SEEK);

      my $buf;
      # valid id nkeys key1...keyn sort data
      # read (valid, id, nkeys)
      my $len=3*$I->_intsize;
      sysread($fh, $buf, $len)==$len or $I->_e(E_READ);
      my ($valid, $id, $nkeys)=unpack $I->intfmt.'3', $buf;

      $pos+=$I->_intsize*(5+$nkeys);
      redo LOOP unless ($valid);

      $len=($nkeys+2)*$I->_intsize; # keys + sort + data
      sysread($fh, $buf, $len)==$len or $I->_e(E_READ);
      my @l=unpack $I->intfmt.'*', $buf;
      my $data=pop @l;
      my $sort=pop @l;

      return ([\@l, $sort, $data, $id], $elpos);
    }
  };
}

sub _really_write_index {
  my ($I, $map, $level)=@_;

  my $fh=$I->_tmpfh;
  my $recordlen=1;		# in ints: (1): for subindexes there is one
                                #          position to store

  # find the max. number of positions we have to store
  foreach my $k (keys %$map) {
    if( ref($map->{$k}) eq 'ARRAY' ) {
      # list of data records
      $recordlen=@{$map->{$k}} if @{$map->{$k}}>$recordlen;
    }
    # else: recordlen is initialized with 1. So for subindexes there is
    #       nothing to do
  }
  # each record comes with a header of 2 integers, the key position in the
  # string table and the actual position count of the record. So we have to
  # add 2 to $recordlen.
  $recordlen+=2;

  # the index itself has a 2 integer header, the recordlen and the number
  # of index records that belong to the index.
  my $indexsize=(2*+$recordlen*keys(%$map))*$I->_intsize; # in bytes

  my $pos=sysseek $fh, 0, SEEK_CUR or $I->_e(E_SEEK);

  # make room
  sysseek $fh, $indexsize, SEEK_CUR or $I->_e(E_SEEK);

  # and write subindices after this index
  my $strings=$I->_stringmap;
  my $sfmt=$I->_stringfmt;
  foreach my $v (values %$map) {
    if( ref($v) eq 'HASH' ) {
      # convert the subindex into a position list
      $v=[$I->_really_write_index($v, $level+1)];
    } else {
      # here we already have a position list but it still contains
      # sorting ids.
      @$v=map {
	$_->[1];
      } sort {
	$a->[0] cmp $b->[0];
      } map {
	# fetch sort string from string table
	[unpack('x'.$_->[0].$sfmt, $$strings), $_->[1]];
      } @$v;
    }
  }

  # now write the index
  sysseek $fh, $pos, SEEK_SET or $I->_e(E_SEEK);

  my $fmt=$I->intfmt;

  # write header
  #my $prefix='--'x$level;
  #warn sprintf "$prefix> pos: %#x, size: %#x\n", $pos, $indexsize;
  #warn "$prefix> idx hdr: ".unpack('H*', pack($fmt.'2',
  #					      0+keys(%$map), $recordlen))."\n";

  syswrite $fh, pack($fmt.'2', 0+keys(%$map), $recordlen) or $I->_e(E_WRITE);

  $fmt.=$recordlen;
  # write the records
  foreach my $key (map {
    $_->[0]
  } sort {
    $a->[1] cmp $b->[1];
  } map {
    [$_, unpack('x'.$_.$sfmt, $$strings)];
  } keys %$map) {
    my $v=$map->{$key};

    #warn "$prefix> idx rec: ".unpack('H*', pack($fmt, $key, 0+@$v, @$v))."\n";

    syswrite $fh, pack($fmt, $key, 0+@$v, @$v) or $I->_e(E_WRITE);
  }

  # and seek back to EOF
  sysseek $fh, 0, SEEK_END or $I->_e(E_SEEK);

  #warn sprintf "$prefix> index written @ %#x\n", $pos;

  return $pos;
}

sub _write_index {
  my ($I)=@_;

  my $dataend=sysseek $I->_tmpfh, 0, SEEK_CUR or $I->_e(E_SEEK);
  my %map;
  for( my $it=$I->_fiterator($dataend); my ($el, $pos)=$it->(); ) {
    my $m=\%map;
    my @k=@{$el->[0]};
    while(@k>1 and ref($m) eq 'HASH') {
      my $k=shift @k;
      $m->{$k}={} unless exists $m->{$k};
      $m=$m->{$k};
    }
    $I->_e(E_DUPLICATE) unless ref($m) eq 'HASH';
    $m->{$k[0]}=[] unless defined $m->{$k[0]};
    $I->_e(E_DUPLICATE) unless ref($m->{$k[0]}) eq 'ARRAY';
    # Actually we want to save only positions but they must be ordered.
    # So either keep the order field together with the position here to
    # sort it later or do sort of ordered insert here.
    # The former is simpler. So it's it.
    push @{$m->{$k[0]}}, [$el->[1], $pos];
  }

  return $I->_really_write_index(\%map, 0);
}

sub _write_id_index {
  my ($I)=@_;

  my $map=$I->_idmap;
  my $fmt=$I->intfmt;
  my $fh=$I->_tmpfh;

  my $pos=sysseek $fh, 0, SEEK_CUR or $I->_e(E_SEEK);

  # write header
  #warn "id> idx hdr: ".unpack('H*', pack($fmt, 0+keys(%$map)))."\n";

  syswrite $fh, pack($fmt, 0+keys(%$map)) or $I->_e(E_WRITE);

  $fmt.='2';
  # write the records
  foreach my $key (sort {$a <=> $b} keys %$map) {
    my $v=$map->{$key};

    #warn "id> idx rec: ".unpack('H*', pack($fmt, $key, $v))."\n";

    syswrite $fh, pack($fmt, $key, $v) or $I->_e(E_WRITE);
  }

  #warn sprintf "id> index written @ %#x\n", $pos;

  return $pos;
}

sub invalidate {
  my ($I)=@_;
  substr( ${$I->_rwdata}, 0, 1 )="\0"; # invalidate current map
}

sub commit {
  my ($I, $dont_invalidate)=@_;

  $I->_ct;
  my $fh=$I->_tmpfh;

  # write NEXTID
  sysseek $fh, BASEOFFSET+NEXTID*$I->_intsize, SEEK_SET or $I->_e(E_SEEK);
  syswrite $fh, pack($I->intfmt, $I->_nextid) or $I->_e(E_WRITE);
  # and move back to where we came from
  sysseek $fh, 0, SEEK_END or $I->_e(E_SEEK);

  # write MAINIDX and IDIDX
  my $mainidx=$I->_write_index;
  my $ididx=$I->_write_id_index;

  sysseek $fh, BASEOFFSET+MAINIDX*$I->_intsize, SEEK_SET or $I->_e(E_SEEK);
  syswrite $fh, pack($I->intfmt, $mainidx) or $I->_e(E_WRITE);
  sysseek $fh, BASEOFFSET+IDIDX*$I->_intsize, SEEK_SET or $I->_e(E_SEEK);
  syswrite $fh, pack($I->intfmt, $ididx) or $I->_e(E_WRITE);

  # write string table
  my $strtbl=sysseek $fh, 0, SEEK_END or $I->_e(E_SEEK);
  {
    my $strings=$I->_stringmap;
    syswrite $fh, $$strings or $I->_e(E_WRITE) if length $$strings;
  }

  sysseek $fh, BASEOFFSET+STRINGTBL*$I->_intsize, SEEK_SET or $I->_e(E_SEEK);
  syswrite $fh, pack($I->intfmt, $strtbl) or $I->_e(E_WRITE);

  #warn "mainidx=$mainidx, ididx=$ididx, strtbl=$strtbl\n";

  undef $I->_idmap;
  undef $I->_strpos;
  undef $I->_stringmap;

  close $I->_stringfh or $I->_e(E_CLOSE);
  undef $I->_stringfh;
  unlink $I->_tmpname.'.strings';

  close $fh or $I->_e(E_CLOSE);
  undef $I->_tmpfh;

  # rename is (at least on Linux) an atomic operation
  rename $I->_tmpname, $I->filename  or $I->_e(E_RENAME);

  $I->invalidate unless $dont_invalidate;

  if( $I->lockfile ) {
    flock $I->lockfile, LOCK_UN or die E_LOCK;
  }

  $I->start;
}

sub _rollback {
  my ($I)=@_;

  close $I->_tmpfh;
  undef $I->_tmpfh;
  unlink $I->_tmpname;

  close $I->_stringfh;
  undef $I->_stringfh;
  unlink $I->_tmpname.'.strings';

  undef $I->_stringmap;
  undef $I->_strpos;
  undef $I->_idmap;

  if( $I->lockfile ) {
    flock $I->lockfile, LOCK_UN or die E_LOCK;
  }
}

sub rollback {
  my ($I)=@_;

  $I->_e(E_TRANSACTION) unless defined $I->_tmpfh;
  $I->_rollback;
}

sub backup {
  my ($I, $fn)=@_;

  $I->start;
  my $backup=$I->new(filename=>(defined $fn ? $fn : $I->filename.'.BACKUP'));
  $backup->begin;
  $backup->commit(1);
}

sub restore {
  my ($I, $fn)=@_;

  $I->start;
  $fn=$I->filename.'.BACKUP' unless defined $fn;

  # rename is (at least on Linux) an atomic operation
  rename $fn, $I->filename  or die E_RENAME;
  $I->invalidate;

  return $I->start;
}

# Returns the position of $key in the stringtable
# If $key is not found it is inserted. @{$I->_strpos} is kept ordered.
# So, we can do a binary search.
# This implements something very similar to a HASH. So, why not use a HASH?
# A HASH is kept completely in core and the memory is not returned to the
# operating system when finished. The number of strings in the database
# can become quite large. So if a long running process updates the database
# only once it will consume much memory for nothing. To avoid this we map
# the string table currently under construction in a separate file that
# is mmapped into the address space of this process and keep here only
# a list of pointer into this area. When the transaction is committed the
# memory is returned to the OS. But on the other hand we need fast access.
# This is achieved by the binary search.
sub _string2pos {
  my ($I, $key)=@_;

  my $strings=$I->_stringmap;

  my $fmt=$I->_stringfmt;
  my $poslist=$I->_strpos;

  my ($low, $high)=(0, 0+@$poslist);
  #warn "_string2pos($key): low=$low, high=$high\n";

  my ($cur, $rel, $curstr);
  while( $low<$high ) {
    $cur=($high+$low)/2;	# "use integer" is active, see above
    $curstr=unpack 'x'.$poslist->[$cur].$fmt, $$strings;
    #warn "  --> looking at $curstr: low=$low, high=$high, cur=$cur\n";
    $rel=($curstr cmp $key);
    if( $rel<0 ) {
      #warn "  --> moving low: $low ==> ".($cur+1)."\n";
      $low=$cur+1;
    } elsif( $rel>0 ) {
      #warn "  --> moving high: $high ==> ".($cur)."\n";
      # don't try to optimize here: $high=$cur-1 will not work in border cases
      $high=$cur;
    } else {
      #warn "  --> BINGO\n";
      return $poslist->[$cur];
    }
  }
  #warn "  --> NOT FOUND\n";
  my $fh=$I->_stringfh;
  my $pos=sysseek $fh, 0, SEEK_CUR or $I->_e(E_SEEK);
  splice @$poslist, $low, 0, $pos+0;
  syswrite $fh, pack($fmt, $key) or $I->_e(E_WRITE);

  # remap $I->_stringmap
  undef $I->_stringmap;
  undef $strings;
  eval {map_handle $strings, $I->_stringfh, '<'}; $I->_e(E_OPEN) if $@;
  $I->_stringmap=\$strings;

  return $pos;
}

sub insert {
  my ($I, $rec)=@_;
  #my ($I, $key, $sort, $data, $id)=@_;

  $I->_ct;

  $rec->[0]=[$rec->[0]] unless ref $rec->[0];
  for my $v (@{$rec}[1,2]) {
    $v='' unless defined $v;
  }

  # create new ID if necessary
  my $id=$rec->[3];
  my $idmap=$I->_idmap;
  if( defined $id ) {
    $I->_e(E_TWICE) if exists $idmap->{$id};
  } else {
    $id=$I->_nextid;
    undef $idmap->{$id};	# allocate it

    my $mask=do{no integer; unpack( $I->intfmt, pack $I->intfmt, -1 )>>1};
    my $nid=($id+1)&$mask;
    $nid=1 if $nid==0;
    while(exists $idmap->{$nid}) {
      $nid=($nid+1)&$mask; $nid=1 if $nid==0;
      $I->_e(E_FULL) if $nid==$id;
    }
    $I->_nextid=$nid;
  }

  my $fh=$I->_tmpfh;

  my $pos=sysseek $fh, 0, SEEK_CUR or $I->_e(E_SEEK);
  # valid id nkeys key1...keyn sort data
  syswrite $fh, pack($I->intfmt.'*', 1, $id, 0+@{$rec->[0]},
		     map {$I->_string2pos($_)} @{$rec->[0]}, @{$rec}[1,2])
    or $I->_e(E_WRITE);

  $idmap->{$id}=$pos;

  return ($id, $pos);
}

sub delete_by_id {
  my ($I, $id, $return_element)=@_;

#   warn "delete_by_id($id)\n";

  # no such id
  return unless exists $I->_idmap->{$id};

  my $fh=$I->_tmpfh;
  my $idmap=$I->_idmap;
  my $pos;

  return unless exists $idmap->{$id};

  $pos=delete $idmap->{$id};

  sysseek $fh, $pos, SEEK_SET or $I->_e(E_SEEK);

  my $buf;
  # read VALID, ID, NKEYS
  my $len=3*$I->_intsize;
  sysread($fh, $buf, $len)==$len or $I->_e(E_READ);
  my ($valid, $elid, $nkeys)=unpack $I->intfmt.'3', $buf;

  return unless $valid;
  return unless $id==$elid; # XXX: should'nt that be an E_CORRUPT

  my $rc=1;
  if( $return_element ) {
    $len=($nkeys+2)*$I->_intsize; # keys + sort + data
    sysread($fh, $buf, $len)==$len or $I->_e(E_READ);

    my $strings=$I->_stringmap;
    my $sfmt=$I->_stringfmt;
    my @l=map {
      unpack('x'.$_.$sfmt, $$strings);
    } unpack($I->intfmt.($nkeys+2), $buf);

    my $rdata=pop @l;
    my $rsort=pop @l;

    $rc=[\@l, $rsort, $rdata, $id];
  }

  sysseek $fh, $pos, SEEK_SET or $I->_e(E_SEEK);
  syswrite $fh, pack($I->intfmt, 0) or $I->_e(E_WRITE);
  sysseek $fh, 0, SEEK_END or $I->_e(E_SEEK);

  return $rc;
}

sub clear {
  my ($I)=@_;

  $I->_ct;

  my $fh=$I->_tmpfh;
  sysseek $fh, BASEOFFSET+DATASTART*$I->_intsize, SEEK_SET or $I->_e(E_SEEK);
  truncate $fh, BASEOFFSET+DATASTART*$I->_intsize or $I->_e(E_TRUNCATE);;

  $fh=$I->_stringfh;
  sysseek $fh, 0, SEEK_SET or $I->_e(E_SEEK);
  truncate $fh, 0 or $I->_e(E_TRUNCATE);;
  $I->_stringmap=\(my $dummy='');

  $I->_idmap={};
  $I->_strpos=[];

  return;
}

# sub xdata_record {
#   my ($I, $pos)=@_;

#   return unless $pos>0 and $pos<$I->mainidx;

#   # valid id nkeys key1...keyn sort data
#   my ($id, $nkeys)=unpack('x'.($pos+$I->_intsize).' '.$I->intfmt.'3',
# 			  ${$I->_data});

#   my $off=$I->_stringtbl;
#   my $data=$I->_data;
#   my $sfmt=$I->_stringfmt;
#   my @l=map {
#     unpack('x'.($off+$_).$sfmt, $$data);
#   } unpack('x'.($pos+3*$I->_intsize).' '.$I->intfmt.($nkeys+2), $$data);

#   my $rdata=pop @l;
#   my $rsort=pop @l;

#   #warn "data_record: keys=[@l], sort=$rsort, data=$rdata, id=$id\n";
#   return [\@l, $rsort, $rdata, $id];
# }

sub iterator {
  my ($I, $show_invalid)=@_;

  return sub {} unless $I->_data;

  my $pos=BASEOFFSET+DATASTART*$I->_intsize;
  my $end=$I->mainidx;

  return sub {
  LOOP: {
      return if $pos>=$end;

      # valid id nkeys key1...keyn sort data
      my ($valid, undef, $nkeys)=
	unpack 'x'.$pos.' '.$I->intfmt.'3', ${$I->_data};

      if( $valid xor $show_invalid ) {
	my $rc=$pos;
	$pos+=$I->_intsize*($nkeys+5); # 5=(valid id nkeys sort data)
	return $rc;
      }
      $pos+=$I->_intsize*($nkeys+5); # 5=(valid id nkeys sort data)
      redo LOOP;
    }
  };
}

#######################################################################
# High Level Accessor Classes
#######################################################################

package
  MMapDB::_base;

use strict;
use Carp qw/croak/;

sub new {
  my ($class, @param)=@_;
  $class=ref($class) || $class;
  return bless \@param=>$class;
}

sub readonly {croak "Modification of a read-only value attempted";}

#######################################################################
# Normal Index Accessor
#######################################################################

package MMapDB::Index;

use strict;
use constant {
  PARENT=>0,
  POS=>1,
  ITERATOR=>2,
};

*TIEHASH=\&MMapDB::_base::new;
*STORE=\&MMapDB::_base::readonly;
*DELETE=\&MMapDB::_base::readonly;
*CLEAR=\&MMapDB::_base::readonly;

sub FETCH {
  my ($I, $key)=@_;
  my @el=$I->[PARENT]->index_lookup($I->[POS], $key);

  return unless @el;

  my $rc;

  if( @el==1 and $el[0]>=$I->[PARENT]->mainidx ) {
    # another index
    tie %{$rc={}}, ref($I), $I->[PARENT], $el[0];
  } else {
    tie @{$rc=[]}, 'MMapDB::Data', $I->[PARENT], \@el;
  }

  return $rc;
}

sub EXISTS {
  my ($I, $key)=@_;
  return $I->[PARENT]->index_lookup($I->[POS], $key) ? 1 : undef;
}

sub FIRSTKEY {
  my ($I)=@_;
  my @el=($I->[ITERATOR]=$I->[PARENT]->index_iterator($I->[POS]))->();
  return @el ? $el[0] : ();
}

sub NEXTKEY {
  my ($I)=@_;
  my @el=$I->[ITERATOR]->();
  return @el ? $el[0] : ();
}

sub SCALAR {
  my ($I)=@_;
  my $pos=defined $I->[POS] ? $I->[POS] : $I->[PARENT]->_ididx;
  my $n=unpack 'x'.$pos.$I->[PARENT]->intfmt,${$I->[PARENT]->_data};
  return $n==0 ? $n : "$n/$n";
}

#######################################################################
# ID Index Accessor
#######################################################################

package MMapDB::IDIndex;

use strict;
use constant {
  PARENT=>0,
  ITERATOR=>2,
};

{our @ISA=qw/MMapDB::Index/}

sub FETCH {
  $_[0]->[PARENT]->data_record($_[0]->[PARENT]->id_index_lookup($_[1]));
}

sub EXISTS {
  my ($I, $key)=@_;
  return $I->[PARENT]->id_index_lookup($key) ? 1 : undef;
}

sub FIRSTKEY {
  my ($I)=@_;
  my @el=($I->[ITERATOR]=$I->[PARENT]->id_index_iterator)->();
  return @el ? $el[0] : ();
}

#######################################################################
# Data Accessor
#######################################################################

package MMapDB::Data;

use strict;
use constant {
  PARENT=>0,
  POSLIST=>1,
};

*TIEARRAY=\&MMapDB::_base::new;
*STORE=\&MMapDB::_base::readonly;
*STORESIZE=\&MMapDB::_base::readonly;
*EXTEND=\&MMapDB::_base::readonly;
*DELETE=\&MMapDB::_base::readonly;
*CLEAR=\&MMapDB::_base::readonly;
*PUSH=\&MMapDB::_base::readonly;
*UNSHIFT=\&MMapDB::_base::readonly;
*POP=\&MMapDB::_base::readonly;
*SHIFT=\&MMapDB::_base::readonly;
*SPLICE=\&MMapDB::_base::readonly;

sub FETCH {
  my ($I, $idx)=@_;
  return unless @{$I->[POSLIST]}>$idx;
  return $I->[PARENT]->data_record($I->[POSLIST]->[$idx]);
}

sub FETCHSIZE {scalar @{$_[0]->[POSLIST]}}

sub EXISTS {@{$_[0]->[POSLIST]}>$_[1]}

1;
__END__

=encoding utf-8

=head1 NAME

MMapDB - a simple database in shared memory

=head1 SYNOPSIS

  use MMapDB qw/:error/;

  # create a database
  my $db=MMapDB->new(filename=>$path, intfmt=>'J');
  $db->start;       # check if the database exists and connect
  $db->begin;       # begin a transaction

  # insert something
  ($id, $pos)=$db->insert([[qw/main_key subkey .../],
                           $sort, $data]);
  # or delete
  $success=$db->delete_by_id($id);
  $just_deleted=$db->delete_by_id($id, 1);

  # or forget everything
  $db->clear;

  # make changes visible
  $db->commit;

  # or forget the transaction
  $db->rollback;

  # use a database
  my $db=MMapDB->new(filename=>$path);
  $db->start;

  # tied interface
  ($keys, $sort, $data, $id)=@{$db->main_index->{main_key}->{subkey}};
  $subindex=$db->main_index->{main_key};
  @subkeys=keys %$subindex;
  @mainkeys=keys %{$db->main_index};

  # or even
  use Data::Dumper;
  print Dumper($db->main_index); # dumps the whole database

  # access by ID
  ($keys, $sort, $data, $id)=@{$db->id_index->{$id}};

  # fast access
  @positions=$db->index_lookup($db->mainidx, $key);
  if( @positions==1 and $positions[0] >= $db->mainidx ) {
    # found another index
    @positions=$db->index_lookup($positions[0], ...);
  } elsif(@positions) {
    # found a data record
    for (@positions) {
      ($keys, $sort, $data, $id)=@{$db->data_record($_)};
    }
  } else {
    # not found
  }

  # access by ID
  $position=$db->id_index_lookup($id);
  ($keys, $sort, $data, $id)=@{$db->data_record($position)};

  # iterate over all valid data records
  for( $it=$db->iterator; $pos=$it->(); ) {
    ($keys, $sort, $data, $id)=@{$db->data_record($pos)};
  }

  # or all invalid data records
  for( $it=$db->iterator(1); $pos=$it->(); ) {
    ($keys, $sort, $data, $id)=@{$db->data_record($pos)};
  }

  # iterate over an index
  for( $it=$db->index_iterator($db->mainidx);
       ($partkey, @positions)=$it->(); ) {
    ...
  }

  # and over the ID index
  for( $it=$db->id_index_iterator;
       ($id, $position)=$it->(); ) {
    ...
  }

  # disconnect from a database
  $db->stop;

=head1 DESCRIPTION

C<MMapDB> implements a database similar to a hash of hashes
of hashes, ..., of arrays of data.

It's main design goals were:

=over 4

=item * very fast read access

=item * lock-free read access for massive parallelism

=item * minimal memory consumption per accessing process

=item * transaction based write access

=item * simple backup, compactness, one file

=back

The cost of write access was unimportant and the expected database size was
a few thousands to a few hundreds of thousands data records.

Hence come 2 major decisions. Firstly, the database is completely mapped
into each process's address space. And secondly, a transaction writes the
complete database anew.

Still interested?

=head1 CONCEPTS

=head2 The data record

A data record consists of 3-4 fields:

 [[KEY1, KEY2, ..., KEYn], ORDER, DATA, ID]   # ID is optional

All of the C<KEY1>, ..., C<KEYn>, C<SORT> and C<DATA> are arbitrary length
octet strings. The key itself is an array of strings showing the way to
the data item. The word I<key> in the rest of this text refers to such
an array of strings.

Multiple data records can be stored under the same key. So, there is
perhaps an less-greater relationship between the data records. That's why
there is the C<ORDER> field. If the order field of 2 or more data records
are equal (C<eq>, not C<==>), their order is defined by the stability of
perl's C<sort> operation. New data records are always appended to the set
of records. So, if C<sort> is stable they appear at the end of a range of
records with the same C<ORDER>.

The C<DATA> field contains the data itself.

A data record in the database further owns an ID. The ID uniquely identifies
the data record. It is assigned when the record is inserted.

An ID is a fixed size number (32 or 64 bits) except 0. They are allocated
from 1 upwards. When the upper boundary is reached the next ID becomes 1
if it is not currently used.

=head2 The index record

An index record consists of 2 or more fields:

 (PARTKEY, POS1, POS2, ..., POSn)

C<PARTKEY> is one of the C<KEYi> that form the key of a data record. The
C<POSi> are positions in the database file that point to other indices
or data records. Think of such a record as an array of data records or
the leafes in the hash of hashes tree.

If an index record contains more than 1 positions they must all point to
data records. This way an ordered array of data records is formed. The position
order is determined by the C<ORDER> fields of the data records involved.

If the index record contains only one position it can point to another
index or a data record. The distiction is made by the so called I<main index>
position. If the position is lower it points to a data record otherwise
to an index.

=head2 The index

An index is a list of index records ordered by their C<PARTKEY>
field. Think of an index as a hash or a twig in the hash of hashes tree.
When a key is looked up a binary search in the index is performed.

There are 2 special indices, the main index and the ID index. The positions
of both of them are part of the database header. This fact is the only
thing that makes the main index special. The ID index is special also
because it's keys are IDs rather than strings.

=head2 The hash of hashes

To summarize all of the above, the following data structure displays the
logical structure of the database:

 $main_index={
               KEY1=>{
                       KEY11=>[[DATAREC1],
                               [DATAREC2],
                               ...],
                       KEY12=>[[DATAREC1],
                               [DATAREC2],
                               ...],
                       KEY13=>{
                                KEY131=>[[DATAREC1],
                                         [DATAREC2],
                                         ...],
                                KEY132=>...
                              },
                     },
               KEY1=>[[DATAREC1],
                      [DATAREC2],
                      ...]
             }

What cannot be expressed is an index record containing a pointer
to a data record and to another subindex, somthing like this:

  KEY=>[[DATAREC],
        {                     # INVALID
          SUBKEY=>...
        },
        [DATAREC],
        ...]

=head2 Accessing a database

To use a database it must be connected. Once connected a database
is readonly. You will always read the same values regardless of other
transactions that may have written the database.

The database header contains a flag that says if it is still valid or
has been replaced by a transaction. There is a method that checks this
flag and reconnects to the new version if necessary. The points when
to call this method depend on your application logic.

=head2 Accessing data

To access data by a key first thing the main index position is read from
the database header. Then a binary search is performed to look up the
index record that matches the first partial key. If it could be found
and there is no more partial key the position list is returned. If there
is another partial key but the position list points to data records the
key is not found. If the position list contains another index position
and there is another partial key the lookup is repeated on this index
with the next partial key until either all partial keys have been found
or one of them could not be found.

A method is provided to read a single data record by its position.

The index lookup is written in C for speed. All other parts are perl.

=head2 Transaction

When a transaction is started a new private copy of the database is
created. Then new records can be inserted or existing ones deleted.
While a transaction is active it is not possible to reconnect to the
database. This ensures that the transaction derives only from the data
that was valid when the transaction has begun.

Newly written data cannot be read back within the transaction. It becomes
visible only when the transaction is committed. So, one will always read
the state from the beginning of the transaction.

When a transaction is committed the public version is replaced by the
private one. Thus, the old database is deleted. But other processes
including the one performing the commit still have the old version still
mapped in their address space. At this point in time new processes will
see the new version while existing see the old data. Therefore a flag
in the old memory mapped database is written signaling that is has become
invalid. The atomicity of this operation depends on the atomicity of
perls C<rename> operation. On Linux it is atomic in a sense that there is
no point in time when a new process won't find the database file.

A lock file can be used to serialize transactions.

=head2 The integer format

All numbers and offsets within the database are written in a certain
format (byte order plus size). When a database is created the format
is written to the database header and cannot be changed afterwards.

Perl's C<pack> command defines several ways of encoding integer numbers.
The database format is defined by one of these pack format letters:

=over 4

=item * L

32 bit in native byte order.

=item * N

32 bit in big endian order. This format should be portable.

=item * J

32 or 64 bit in native byte order.

=item * Q

64 bit in native byte order. But is not always implemented.

=back

=head2 Iterators

Some C<MMapDB> methods return iterators. An iterator is a function
reference (also called closure) that if called returns one item at a time.
When there are no more items an empty list or C<undef> is returned.

If you haven't heard of this concept yet perl itself has an iterator
attached to each hash. The C<each> operation returns a key/value pair
until there is no more.

Iterators are used this way:

  $it=$db->...;       # create an iterator
  while( @item=$it->() ) {
    # use @item
  }

=head2 Error Conditions

Some programmers believe exceptions are the right method of error reporting.
Other think it is the return value of a function or method.

This modules somehow divides errors in 2 categories. A key that could not
be found for example is a normal result not an error. However, a disk full
condition or unsufficient permissions to create a file are errors. This
kind of errors are thrown as exceptions. Normal results are returned through
the return value.

If an exception is thrown in this module C<$@> will contain a scalar reference.
There are several errors defined that can be imported into the using
program:

  use MMapDB qw/:error/;  # or :all

The presence of an error is checked this way (after C<eval>):

  $@ == E_TRANSACTION

Human readable error messages are provided by the scalar C<$@> points to:

  warn ${$@}

=head3 List of error conditions

=over 4

=item * E_READONLY

database is read-only

=item * E_TWICE

attempt to insert the same ID twice

=item * E_TRANSACTION

attempt to begin a transaction while there is one active

=item * E_FULL

no more IDs

=item * E_DUPLICATE

data records cannot be mixed up with subindices

=item * E_OPEN

can't open file

=item * E_READ

can't read from file

=item * E_WRITE

can't write to file

=item * E_CLOSE

file could not be closed

=item * E_RENAME

can't rename file

=item * E_SEEK

can't move file pointer

=item * E_TRUNCATE

can't truncate file

=item * E_LOCK

can't lock or unlock

=back

=head1 METHODS

=head2 $dh=MMapDB-E<gt>new( KEY=E<gt>VALUE, ... )

=head2 $new=$db-E<gt>new( KEY=E<gt>VALUE, ... )

creates or clones a database handle. If there is an active transaction
it is rolled back for the clone.

Parameters are passed as (KEY,VALUE) pairs:

=over 4

=item * filename

specifies the database file

=item * lockfile

specify a lockfile to serialize transactions. If given at start of a
transaction the file is locked using C<flock> and unlocked at commit
or rollback time. The lockfile is empty but must not be deleted. Best
if it is created before first use.

If C<lockfile> ommitted C<MMapDB> continues to work but it is possible
to begin a new transaction while another one is active in another process.

=item * intfmt

specifies the integer format. Valid values are C<N> (the default), C<L>,
C<J> and C<Q> (not always implemented). When opening an existing database
this value is overwritten by the database format.

=item * readonly

if passed a true value the database file is mapped read-only. That means
the accessing process will receive a SEGV signal (on UNIX) if some stray
pointer wants to write to this area. At perl level the variable is marked
read-only. So any read access at perl level will not generate a segmentation
fault but instead a perl exception.

=back

=head2 $success=$db-E<gt>set_intfmt(LETTER)

sets the integer format of an existing database handle. Do not call this
while the handle is connected to a database.

=head2 $db-E<gt>filename

=head2 $db-E<gt>readonly

=head2 $db-E<gt>intfmt

those are accessors for the attributes passed to the constructor. They can
also be assigned to (C<$db-E<gt>filename=$new_name>) but only before
a database is connected to. C<intfmt> must be set using C<set_intfmt()>.

=head2 $success=$db-E<gt>start

(re)connects to the database. This method maps the database file and checks
if it is still valid. This method cannot be called while a transaction
is active.

If the current database is still valid or the database has been successfully
connected or reconnected the database object is returned. C<undef> otherwise.

There are several conditions that make C<start> fail:

=over 4

=item * $db-E<gt>filename could not be opened

=item * the file could not be mapped into the address space

=item * the file is empty

=item * the magic number of the file does not match

=item * the integer format indentifier is not valid

=back

=head2 $success=$db-E<gt>stop

disconnects from a database.

=head2 $success=$db-E<gt>begin

begins a transaction

=head2 $success=$db-E<gt>commit

=head2 $success=$db-E<gt>commit(1)

commits a transaction and reconnects to the new version.

Returns the database object it the new version has been connected.

If a true value is passed to this message the old database version is
not invalidated. This makes it possible to safely create a copy for backup
this way:

  my $db=MMapDB->new(filename=>FILENAME);
  $db->start;
  $db->filename=BACKUPNAME;
  $db->begin;
  $db->commit(1);

or

  my $db=MMapDB->new(filename=>FILENAME);
  $db->start;
  my $backup=$db->new(filename=>BACKUP);
  $backup->begin;
  $backup->commit(1);

=head2 $success=$db-E<gt>rollback

forgets about the current transaction

=head2 $db-E<gt>is_valid

returns true if a database is connected an valid.

=head2 $db-E<gt>invalidate

invalidates the current version of the database. This is normally called
internally by C<commit> as the last step. Perhaps there are also other
uses.

=head2 $db-E<gt>backup(BACKUPNAME)

creates a backup of the current database version called C<BACKUPNAME>.
It works almost exactly as shown in L<commit|/$success=$db-E<gt>commit(1)>.
Note, C<backup> calls C<$db-E<gt>start>.

If the C<filename> parameter is ommitted the result of appending the
C<.BACKUP> extension to the object's C<filename> property is used.

=head2 $db-E<gt>restore(BACKUPNAME)

just the opposite of C<backup>. It renames C<BACKUPNAME> to
C<$db-E<gt>filename> and invalidates the current version. So, the backup
becomes the current version. For other processes running in parallel this
looks just like another transaction being committed.

=head2 @positions=$db-E<gt>index_lookup(INDEXPOS, KEY1, KEY2, ...)

looks up the key C<[KEY1, KEY2, ...]> in the index given by its position
C<INDEXPOS>. Returns a list of positions.

To check if the result is a data record array or another index use this code:

  if( @positions==1 and $positions[0] >= $db->mainidx ) {
    # found another index
    # the position can be passed to another index_lookup()
  } elsif(@positions) {
    # found a data record
    # the positions can be passed to data_record()
  } else {
    # not found
  }

=head2 $position=$db-E<gt>id_index_lookup(ID)

looks up a data record by its ID. Returns the data record's position.

=head2 $rec=$db-E<gt>data_record(POS)

given a position fetches a data record. C<$res> is an array reference
with the following structure:

  [[KEY1, KEY2, ...], SORT, DATA, ID]

All of the C<KEYs>, C<SORT> and C<DATA> are read-only strings.

=head2 $it=$db-E<gt>iterator

=head2 $it=$db-E<gt>iterator(1)

perhaps you want to iterate over all data records in a database.
The iterator returns a data record position:

  $position=$it->()

If a true value is passed as parameter only deleted records are found
otherwise only valid ones.

=head2 $it=$db-E<gt>index_iterator(POS)

iterate over an index given by its position. The iterator returns
a partial key and a position list:

  ($partkey, @positions)=$it->()

=head2 $it=$db-E<gt>id_index_iterator

iterate over the ID index. The iterator returns 2 elements, the ID
and the data record position:

  ($id, $position)=$it->()

=head2 ($id, $pos)=$db-E<gt>insert([[KEY1, KEY2, ....], SORT, DATA])

=head2 ($id, $pos)=$db-E<gt>insert([[KEY1, KEY2, ....], SORT, DATA, ID])

insert a new data record into the database. The ID parameter is optional
and should be ommitted in most cases. If it is ommitted the next available
ID is allocated and bound to the record. The ID and the position in the
new database version are returned. Note, that the position cannot be used
as parameter to the C<data_record> method until the transaction is
committed.

=head2 $success=$db-E<gt>delete_by_id(ID)

=head2 ($keys, $sort, $data, $id)=@{$db-E<gt>delete_by_id(ID, 1)}

delete an data record by its ID. If the last parameter is true the
deleted record is returned if there is one. Otherwise only a status
is returned whether a record has been deleted by the ID or not.

=head2 $db-E<gt>clear

deletes all data records from the database.

=head2 $pos=$db-E<gt>mainidx

returns the main index position. You probably need this as parameter
to C<index_lookup()>.

=head2 $hashref=$db-E<gt>main_index

This is an alternative way to access the database via tied hashes.

  $db->main_index->{KEY1}->{KEY2}->[0]

returns almost the same as

  $db->data_record( ($db->index_lookup($db->mainidx, KEY1, KEY2))[0] )

provided C<[KEY1, KEY2]> point to an array of data records.

While it is easier to access the database this way it is also much slower.

=head2 $hashref=$db-E<gt>id_index

The same works for indices:

  $db->id_index->{42}

returns almost the same as

  $db->data_record( $db->id_index_lookup(42) )

=head2 EXPORT

None by default.

Error constants are imported by the C<:error> tag.

=head1 READ PERFORMANCE

The C<t/002-benchmark.t> test as the name suggests is mainly about
benchmarking. It is run only if the C<BENCHMARK> environment
variable is set. E.g. on a C<bash> command line:

 BENCHMARK=1 make test

It creates a
database with 10000 data records in a 2-level hash of hashes structure.
Then it finds the 2nd level hash with the largest number of elements and
looks for one of the keys there.

This is done for each database format using both C<index_lookup> and
the tied interface. For comparison 2 perl hash lookups are also
measured:

 sub {(sub {scalar @{$c->{$_[0]}->{$_[1]}}})->($k1, $k2)};
 sub {scalar @{$c->{$k1}->{$k2}}};

As result you can expect something like this:

             Rate mmdb_L mmdb_N mmdb_Q mmdb_J hash1 idxl_N idxl_Q idxl_J idxl_L hash2
 mmdb_L   41489/s     --    -0%    -1%    -1%  -89%   -92%   -93%   -93%   -93%  -99%
 mmdb_N   41696/s     1%     --    -0%    -1%  -89%   -92%   -93%   -93%   -93%  -99%
 mmdb_Q   41755/s     1%     0%     --    -1%  -89%   -92%   -93%   -93%   -93%  -99%
 mmdb_J   42011/s     1%     1%     1%     --  -89%   -92%   -93%   -93%   -93%  -99%
 hash1   380075/s   816%   812%   810%   805%    --   -31%   -32%   -33%   -33%  -87%
 idxl_N  548741/s  1223%  1216%  1214%  1206%   44%     --    -2%    -3%    -4%  -81%
 idxl_Q  560469/s  1251%  1244%  1242%  1234%   47%     2%     --    -1%    -2%  -81%
 idxl_J  568030/s  1269%  1262%  1260%  1252%   49%     4%     1%     --    -0%  -81%
 idxl_L  570717/s  1276%  1269%  1267%  1258%   50%     4%     2%     0%     --  -81%
 hash2  2963100/s  7042%  7006%  6996%  6953%  680%   440%   429%   422%   419%    --

The C<mmdb> tests use the tied interface. C<idxl> means C<index_lookup>.
C<hash1> is the first of the 2 perl hash lookups above, C<hash2> the 2nd.

C<hash2> is naturally by far the fastest. But add one anonymous function level and
C<idxl> becomes similar. Further, the C<N> format on this machine requires
byte rearrangement. So, it is expected to be slower. But it differs only in
a few percents from the other C<idxl>.

=head1 BACKUP AND RESTORE

The database consists only of one file. So a backup is in principle a
simple copy operation. But there is a subtle pitfall.

If there are writers active while the copy is in progress it may become
invalid between the opening of the database file and the read of the first
block.

So, a better way is this:

 perl -MMMapDB -e 'MMapDB->new(filename=>shift)->backup' DATABASENAME

To restore a database use this one:

 perl -MMMapDB -e 'MMapDB->new(filename=>shift)->restore' DATABASENAME

See also L<backup()|/$db-E<gt>backup(filename=E<gt>BACKUPNAME)> and
L<restore()|/$db-E<gt>restore(filename=E<gt>BACKUPNAME)>

=head1 DISK LAYOUT

In this paragraph the letter I<S> is used to designate a number of 4 or 8
bytes according to the C<intfmt>. The letter I<F> is used as a variable
holding that format. So, when you see C<F/a*> think C<N/a*> or C<Q/a*>
etc. according to C<intfmt>.

If an integer is used somewhere in the database it is aligned correctly.
So, 4-byte integer values are located at positions divisible by 4 and
8-byte integers are at positions divisible by 8. This also requires strings
to be padded up to the next divisible by 4 or 8 position.

Strings are always packed as C<F/a*> and padded up to the next S byte
boundary. With C<pack> this can be achieved with C<F/a* x!S> where C<F>
is one of C<N>, C<L>, C<J> or C<Q> and C<S> either 4 or 8.

The database can be divided into a few major sections:

=over 4

=item * the database header

=item * the data records

=item * the indices

=item * the string table

=back

=head2 The database header

At start of each database comes a descriptor:

 +----------------------------------+
 | MAGIC NUMBER (4 bytes) == 'MMDB' |
 +----------------------------------+
 | FORMAT (1 byte) + 3 bytes resrvd |
 +----------------------------------+
 | MAIN INDEX POSITION (S bytes)    |
 +----------------------------------+
 | ID INDEX POSITION (S bytes)      |
 +----------------------------------+
 | NEXT AVAILABLE ID (S bytes)      |
 +----------------------------------+
 | STRING TABLE POSITION (S bytes)  |
 +----------------------------------+

The magic number always contains the string C<MMDB>. The C<FORMAT> byte
contains the C<pack> format letter that describes the integer format of the
database. It corresponds to the C<intfmt> property. When a database becomes
invalid a NULL byte is written at this location.

L<Main Index position|/The Index> is the file position just after all
data records where the main index resides.

L<ID Index|/The Index> is the last index written to the file. This field
contains its position.

The C<NEXT AVAILABLE ID> field contains the next ID to be allocated.

The C<STRING TABLE POSITION> keeps the offset of the string table at the end
of the file.

=head2 Data Records

Just after the descriptor follows an arbitrary umber of data records.
In the following diagrams the C<pack> format is shown after most of the
fields. Each record is laid out this way:

 +----------------------------------+
 | VALID FLAG (S bytes)             |
 +----------------------------------+
 | ID (S bytes)                     |
 +----------------------------------+
 | NKEYS (S bytes)                  |
 +----------------------------------+
 ¦                                  ¦
 ¦ KEY POSITIONS (NKEYS * S bytes)  ¦
 ¦                                  ¦
 +----------------------------------+
 | SORT POSITION                    |
 +----------------------------------+
 | DATA POSITION                    |
 +----------------------------------+

A data record consists of a certain number of keys, an ID, a sorting field
and of course the data itself. The key, sort and data positions are offsets
from the start of the string table.

The C<valid> flag is 1 if the data is active or 0 if it has been deleted.
In the latter case the next transaction will purge the database.

=head2 The Index

Just after the data section follows the main index. Its starting position
is part of the header. After the main index an arbitrary number
of subindices may follow. The last index in the database is the ID
index.

An index is mainly an ordered list of strings each of which points to a
list of positions. If an index element points to another index this
list contains 1 element, the position of the index. If it points to
data records the position list is ordered according to the sorting
field of the data items.

An index starts with a short header consisting of 2 numbers followed by
constant length index records:

 +----------------------------------+
 | NRECORDS (S bytes)               |
 +----------------------------------+
 | RECORDLEN (S bytes)              |  in units of integers
 +----------------------------------+
 ¦                                  ¦
 ¦ constant length records          ¦
 ¦                                  ¦
 +----------------------------------+

The record length is a property of the index itself. It is the length of the
longest index record constituting the index. It is expressed in units of C<S>
bytes.

Each record looks like this:

 +----------------------------------+
 | KEY POSITION (S bytes)           |
 +----------------------------------+
 | NPOS (S bytes)                   |
 +----------------------------------+
 ¦                                  ¦
 ¦ POSITION LIST (NPOS * S bytes)   ¦
 ¦                                  ¦
 +----------------------------------+
 ¦                                  ¦
 ¦ padding up to RECORDLEN          ¦
 ¦                                  ¦
 +----------------------------------+

The C<KEY POSITION> contains the offset of the record's partial key from
the start of the string table. C<NPOS> is the number of elements of the
subsequent position list. Each position list element is the starting
point of a data record or another index relativ to the start of the file.

=head2 The string table

The string table is located at the end of the database file that so far
consists only of integers. There is no structure in the string table. All
strings are simply padded to the next integer boundary and concatenated.

Each string is encoded with the C<F/a* x!S> pack-format.

=head1 AUTHOR

Torsten Förtsch, E<lt>torsten.foertsch@gmx.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Torsten Förtsch

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.

=cut
