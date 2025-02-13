import cherry_core
from cherry_core import ingest
import asyncio

signature = "Transfer(address indexed from, address indexed to, uint256 amount)"
topic0 = cherry_core.evm_signature_to_topic0(signature)
contract_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

async def main():
    stream = ingest.start_stream(ingest.StreamConfig(
        format=ingest.Format.EVM,
        provider=ingest.Provider(
            kind=ingest.ProviderKind.SQD,
            config=ingest.ProviderConfig(
                url="https://portal.sqd.dev/datasets/ethereum-mainnet"
            ),
        ),
        query=ingest.EvmQuery(
            from_block=20123123,
            logs=[ingest.LogRequest(
                address=[contract_address],
                #event_signatures=[signature],
                topic0=[topic0], # same effect as above
            )],
            fields=ingest.Fields(
                block=ingest.BlockFields(
                    number=True,
                ),
                log=ingest.LogFields(
                    data=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                )
            )
        ),
    ))

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res["blocks"].column("number"))

        decoded = cherry_core.evm_decode_events(signature, res["logs"])
        print(decoded)

asyncio.run(main())