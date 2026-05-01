#!/usr/bin/perl
use strict;
use warnings;
use IO::Socket::UNIX;
use IO::Select;

my $SOCK_PATH = "/tmp/.wst.sock";

$| = 1;
binmode STDIN;
binmode STDOUT;

my $sock = IO::Socket::UNIX->new(
    Type => SOCK_STREAM,
    Peer => $SOCK_PATH,
) or die "connect: $!\n";

# Wait for TUNNEL_READY
my $rbuf = "";
while (1) {
    my $sel = IO::Select->new($sock);
    $sel->can_read(5) or exit 1;
    my $n = $sock->sysread(my $data, 4096);
    exit 1 unless $n;
    $rbuf .= $data;
    if ($rbuf =~ s/^(.*?\n)//) {
        syswrite(STDOUT, $1);
        last;
    }
}

$sock->blocking(0);
open(my $stdin_fh, "<&=", 0) or die "fdopen stdin: $!\n";

my ($stdin_buf, $sock_buf) = ("", $rbuf);

while (1) {
    my @rlist = ($stdin_fh, $sock);
    my @wlist;
    push @wlist, $sock   if length $stdin_buf;
    push @wlist, \*STDOUT if length $sock_buf;

    my $sr = IO::Select->new(@rlist);
    my $sw = IO::Select->new(@wlist);

    my ($rd, $wr, undef) = IO::Select->select($sr, $sw, undef, 1);

    if ($wr) {
        for my $s (@$wr) {
            if ($s == \*STDOUT && length $sock_buf) {
                my $n = syswrite(STDOUT, $sock_buf);
                last unless defined $n;
                $sock_buf = substr($sock_buf, $n);
            }
            if ($s == $sock && length $stdin_buf) {
                my $n = $sock->syswrite($stdin_buf);
                last unless defined $n;
                $stdin_buf = substr($stdin_buf, $n);
            }
        }
    }

    if ($rd) {
        for my $s (@$rd) {
            if ($s == $stdin_fh) {
                my $n = sysread($stdin_fh, my $data, 4096);
                exit 0 unless $n;
                $stdin_buf .= $data;
            }
            elsif ($s == $sock) {
                my $n = $sock->sysread(my $data, 4096);
                exit 0 unless $n;
                $sock_buf .= $data;
            }
        }
    }
}
