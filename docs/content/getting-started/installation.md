---
title: "Installation"
description: "Install aki with Go, Homebrew, Scoop, a Linux package, Docker, or a prebuilt binary."
weight: 20
---

aki is a single static binary with no runtime dependencies.
Pick whichever channel fits your platform.
Every method gives you the same `aki` command.

## Go

If you have Go 1.26 or newer, install straight from source:

```bash
go install github.com/tamnd/aki/cmd/aki@latest
```

This builds the binary into `$(go env GOPATH)/bin`, so make sure that is on your `PATH`.

## Homebrew (macOS and Linux)

```bash
brew install tamnd/tap/aki
```

To upgrade later:

```bash
brew upgrade aki
```

## Scoop (Windows)

```powershell
scoop bucket add tamnd https://github.com/tamnd/scoop-bucket
scoop install aki
```

## Linux packages

Each release ships `.deb`, `.rpm`, and `.apk` packages for amd64 and arm64.
Download the one for your distribution from the [releases page](https://github.com/tamnd/aki/releases) and install it:

```bash
# Debian and Ubuntu
sudo dpkg -i aki_*_linux_amd64.deb

# Fedora, RHEL, and friends
sudo rpm -i aki_*_linux_amd64.rpm

# Alpine
sudo apk add --allow-untrusted aki_*_linux_amd64.apk
```

## Docker

The image is published to the GitHub Container Registry:

```bash
docker run --rm -p 6379:6379 -v "$PWD/data:/data" ghcr.io/tamnd/aki \
  server --dbfile /data/data.aki --addr 0.0.0.0:6379
```

The volume keeps your `.aki` file on the host so the data survives the container.

## Prebuilt binaries

If you would rather not use a package manager, grab a prebuilt archive for your platform from the [releases page](https://github.com/tamnd/aki/releases), unpack it, and put the `aki` binary on your `PATH`.
Builds are published for Linux, macOS, and Windows on both amd64 and arm64.

## Build from source

```bash
git clone https://github.com/tamnd/aki
cd aki
make build      # builds bin/aki
```

aki is pure Go with no cgo, so a plain `go build ./cmd/aki` works too.

## Verify the install

```bash
aki version
```

You should see the version, commit, and build date.
Now run the [quick start](/getting-started/quick-start/).
