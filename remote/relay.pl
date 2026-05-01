#!/usr/bin/perl
use strict;
use warnings;
use IO::Socket::UNIX;
use IO::Select;

my $SOCK_PATH = "/tmp/.wst.sock";

unlink $SOCK_PATH;
$SIG{TERM} = sub { unlink $SOCK_PATH; exit 0; };
END { unlink $SOCK_PATH if -e $SOCK_PATH; }

my $srv = IO::Socket::UNIX->new(
    Type   => SOCK_STREAM,
    Local  => $SOCK_PATH,
    Listen => 2,
) or die "bind: $!\n";

my $a = $srv->accept() or die "accept A: $!\n";
my $b = $srv->accept() or die "accept B: $!\n";

$a->send("TUNNEL_READY\n");
$b->send("TUNNEL_READY\n");

$a->blocking(0);
$b->blocking(0);

my $sel_r = IO::Select->new($a, $b);
my ($buf_a2b, $buf_b2a) = ("", "");

while (1) {
    my @rlist;
    push @rlist, $a if length($buf_a2b) < 65536;
    push @rlist, $b if length($buf_b2a) < 65536;

    my $sr = IO::Select->new(@rlist);
    my @wlist;
    push @wlist, $b if length $buf_a2b;
    push @wlist, $a if length $buf_b2a;
    my $sw = IO::Select->new(@wlist);

    my ($rd, $wr, undef) = IO::Select->select($sr, $sw, undef, 1);

    if ($wr) {
        for my $s (@$wr) {
            if ($s == $b && length $buf_a2b) {
                my $n = $b->syswrite($buf_a2b);
                last unless defined $n;
                $buf_a2b = substr($buf_a2b, $n);
            }
            if ($s == $a && length $buf_b2a) {
                my $n = $a->syswrite($buf_b2a);
                last unless defined $n;
                $buf_b2a = substr($buf_b2a, $n);
            }
        }
    }

    if ($rd) {
        for my $s (@$rd) {
            my $data;
            my $n = $s->sysread($data, 4096);
            if (!$n) {
                # one side closed
                exit 0;
            }
            if ($s == $a) { $buf_a2b .= $data; }
            else          { $buf_b2a .= $data; }
        }
    }
}
