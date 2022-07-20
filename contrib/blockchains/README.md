Blockchains
===========

Note that these are considered old blocks and importing them to the node will leave the node in
an initial block download state. This is a state in which it will not respond to P2P requests from
non-whitelisted connections. Note that this is resolved by whitelisting your IP or generating a new 
block, which will have a recent timestamp, and will exit this state.

If you are running the node in Docker, it seems to use incrementally higher addresses each time it
brings containers up, so it is not possible to whitelist a specific address. Instead we whitelist
172.0.0.0/8 so that all addresses with a 172 prefix can bypass the initial block download state.

blockchain_115_3677f4
---------------------

* Mining regtest wallet.
  * Seed words: entire coral usage young front fury okay fade hen process follow light
  * docker exec conduit-db_node_1 bash -c "python3 /opt/call_any.py generatetoaddress 110 n2ekqiw96ceQWFrKSziKTEi5fsRuZKQdun"
* Funds regtest wallet 1.
  * Seed words: neutral cash ozone buyer cook match exhaust usual purse transfer evil believe
  * Receive P2PK:
    * Transaction id: 88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210
    * Public key: 032fcb2fa3280cfdc0ffd527b40f592f5ae80556f2c9f98a649f1b1af13f332fdb
    * Pushdata = SHA256(public key): 04bca2ae277997940152716854a95347819c2e07d370d22c093b39708fb9d5eb
  * Receive P2PKH:
    * Transaction id: d53a9ebfac748561132e49254c42dbe518080c2a5956822d5d3914d47324e842
    * P2PKH address: mhg6ENXhPL6LsEUG6oxdqi8LjE2bsW6NMW
    * Hash 160: 17aa9ecb9e38b91bddcc5e8d2d26154be90d8996
    * Pushdata = SHA256(hash 160): e351e4d2499786e8a3ac5468cbf1444b3416b41e424524b50e2dafc8f6f454db
  * Spend P2PK/P2PKH: 47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095
    * P2PK = index 0
    * P2PKH = index 1
* Multi-signature wallet 1/2.
  * Seed words: forward jeans speed carpet sadness town foam cigar hunt flight section soap
  * Master public key: tpubD6NzVbkrYhZ4XPgahFuy3RWHUQarUthf98XMhGrRBnWBucqiKzjvFm8ucBtiJkvarWeiGAsiGsK7XThXCNRJSsFPdhy9gRHGF7gVRhRWgnB
  * Receive P2MS:
    * Transaction id: 479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2
    * Input: 2
    * Pushdata = sha256(mpk.child(0).child_safe(0).to_bytes()).hex(): 9ed50dfe0d3a28950ee9a2ee41dce7193dd8666c4ff42c974de1bde60332a701
    * Script: bitcoin-script:010252210310274914ab9e07b507eb13cde158320450e4f6d6508645b1e06069976802a9332103308c167165296c5253c798fe11820a8c3b4245e2d252c9d8c25f6bbf98a9bfa052aeffd208ea
  * Spend P2MS:
    * Transaction id: 0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721
    * Input: 0
    * Destination: mwcrgDbyRSaYaU9PSYDkjJQyPn4f9j5NQg (back to mining wallet)
  * Receive P2SH:
    * Transaction id: 49250a55f59e2bbf1b0615508c2d586c1336d7c0c6d493f02bc82349fabe6609
    * P2SH address: 2N5338aAPYmKM59AKpvxDB6FRAnGNXRsfBp
    * Output: 1
    * Pushdata = SHA256(hash 160): 5e7583878789b03276d2d60a1cf3772a999084e3b12d0d3c1a33a30bd15609db
  * Spend P2SH:
    * Transaction id: 1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6
    * Input: 0
    * Destination: mx1aTpTj9L7Qtd1ixAR9b8BYeBYWsWNUDv (back to mining wallet)
* Multi-signature wallet 2/2.
  * Seed words: country victory shell few security noble moment castle tiny erode divorce become
  * Master public key: tpubD6NzVbkrYhZ4YPUxWgGYpvYrXRjGMxRvG9GgMJpRQCiC5SWUz492QaAAZq4QtAu1NXH3UmVmqwzRR5BbiG6XCAcuy7DYGSLBuxf2miD24qr
  * Receive P2MS (see the 1/2 entry):
    * Pushdata = sha256(mpk.child(0).child_safe(0).to_bytes()).hex(): e6221c70e0f3c686255b548789c63d0e2c6aa795ad87324dfd71d0b53d90d59d


blockchain_116_685adc
---------------------
This represents a reorging of the ElectrumSV transactions 
in blockchain_115_3677f4 (from heights 111-115) to a new block height of 116 
(block hash 685adce71612e34553bed7166e2cfaa3eed6df8ff1c030b7a4977627d8515eab)
See `generate_reorg_blocks.py` for the script that was used to create these blocks

blockchain_117_28c2d3
---------------------
This represents a reorging of the ElectrumSV transactions 
in `blockchain_115_3677f4` (from heights 111-115) to a new block height of 116 
(block hash 685adce71612e34553bed7166e2cfaa3eed6df8ff1c030b7a4977627d8515eab)
- Another random block, height 117 is added ontop to leave room for generating
a block ontop of `blockchain_115_3677f4` to take the node out of initial block
download mode.

blockchain_117_155d19
---------------------
Builds directly on `blockchain_115_3677f4` which contains 2 invoice payments.
One created in our own wallet as the payee (1.3.15) and paid by the mining wallet.
One created in the ElectrumSV BIP270 server as the payee and paid by the
v1.3.15 ElectrumSV wallet.

Receive P2PKH:
* Transaction id: 3b6c64b30c0b3be3a49e4e03daaf946004cc84ca510ae9b191868f9d81eab549
* P2PKH address: mnpSpZRCUgin2pMuaoLgFL4rqSxEAWpXuS
* Hash 160: 5017c39caf3af0c6e0f2bcd0735b6ed072d440df
* Pushdata = SHA256(hash 160):544f6454c8b10e4982fcc34c5003da46f0c69b18581e024ddaa0da8c24d2d6c5

Send P2PKH:
* Transaction id: 23f46fde26d1676f55ae81aafe844618dd7abe7e7bf081ae99146f28a816b002
* P2PKH address: mwi59DBTG6oS6PJWwPFu4YGJjVTvXXXNoF
* Hash 160: b19c4ddcd9ce829b6bab5fc10f60b50b69645b2b
* Pushdata = SHA256(hash 160):3a00577cd91c3e4c517462ae03339a712ac2281616aff66f18c705504d22f4d7
