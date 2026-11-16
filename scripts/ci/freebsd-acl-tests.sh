#!/bin/sh
# Prove the ancestor ACL check on a filesystem that supports native NFSv4 ACLs.
set -eu

[ "$(uname -s)" = FreeBSD ] && [ "$(id -u)" = 0 ] || {
    echo "This fixture requires a FreeBSD root process" >&2
    exit 1
}
for tool in cargo mdconfig newfs mount umount truncate getfacl setfacl timeout; do
    command -v "$tool" >/dev/null || { echo "Missing prerequisite: $tool" >&2; exit 1; }
done

fixture_root=$(mktemp -d /mcpcli-acl.XXXXXX)
fixture_image="$fixture_root/fs.img"
fixture_mount="$fixture_root/mount"
device=
mounted=0
cleanup() {
    status=$?
    trap - 0 HUP INT TERM
    set +e
    failed=0
    if [ "$mounted" = 1 ]; then
        actual=$(mount -p | awk -v path="$fixture_mount" '$2 == path { print $1 }')
        if [ "$actual" = "/dev/$device" ]; then
            umount "$fixture_mount" || failed=1
        else
            echo "Refusing to unmount unexpected device at $fixture_mount" >&2
            failed=1
        fi
    fi
    if [ -n "$device" ] && [ "$failed" = 0 ]; then
        owned=$(mdconfig -l -f "$fixture_image")
        if [ "$owned" = "$device" ]; then
            mdconfig -d -u "$device" || failed=1
        else
            echo "Refusing to detach unverified memory disk $device" >&2
            failed=1
        fi
    fi
    if [ "$failed" = 0 ]; then
        rm -rf "$fixture_root" || failed=1
    fi
    if [ "$failed" != 0 ]; then
        echo "ACL fixture cleanup failed; retained $fixture_root" >&2
        [ "$status" != 0 ] || status=1
    fi
    exit "$status"
}
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

chmod 755 "$fixture_root"
mkdir "$fixture_mount"
truncate -s 64m "$fixture_image"
device=$(mdconfig -a -t vnode -f "$fixture_image")
newfs -U "/dev/$device"
mount -t ufs -o nfsv4acls "/dev/$device" "$fixture_mount"
mounted=1
chmod 755 "$fixture_mount"
test_name=platform::tests::freebsd_ancestor_acl_mutation_is_rejected
if env TMPDIR="$fixture_mount" MCPCLI_REQUIRE_NFS4_ACL=1     timeout 600 cargo test --locked --lib -- "$test_name" --exact --nocapture     > ci-freebsd-acl.log 2>&1; then
    cat ci-freebsd-acl.log
else
    status=$?
    cat ci-freebsd-acl.log
    exit "$status"
fi
grep -Fq "test result: ok. 1 passed; 0 failed; 0 ignored;" ci-freebsd-acl.log || {
    echo "The required FreeBSD ACL regression did not execute" >&2
    exit 1
}
echo "PASS: native NFSv4 ancestor ACL mutation rejection"
