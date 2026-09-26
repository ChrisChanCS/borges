Ubuntu 22.04 VM runtime libraries
==================================

These shared libraries are copied into Ubuntu 22.04 VMs by `scripts/setup.sh VMS`.
They come from Ubuntu Jammy `universe` amd64 packages:

- `libfmt8_8.1.1+ds1-2_amd64.deb`
- `libspdlog1_1.9.2+ds-0.2_amd64.deb`
- `libjemalloc2_5.2.1-4ubuntu1_amd64.deb`

The package SHA256 sums were checked against the Jammy package index before
extracting the libraries.
