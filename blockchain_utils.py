import re
import os
import aiohttp
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class BlockchainUtils:
    """Utility for extracting and verifying Blockchain Contract Addresses (CAs)."""
    
    # Standard Ethereum/Polygon/BSC address regex
    CA_PATTERN = re.compile(r"\b0x[a-fA-F0-9]{40}\b")

    @classmethod
    def extract_cas(cls, text: str) -> List[str]:
        """Extract all unique contract addresses from text."""
        if not text:
            return []
        return list(set(cls.CA_PATTERN.findall(text)))

    @classmethod
    async def verify_ca(cls, ca: str, network: str = "ethereum") -> Dict:
        """
        Verifies if an address is a valid contract and attempt to get its metadata.
        Uses Etherscan or Alchemy APIs if keys are available.
        """
        result = {"is_contract": False, "name": None, "network": network, "verified": False}
        
        # 1. Alchemy Fallback (Enterprise Grade)
        alchemy_key = os.environ.get("ALCHEMY_API_KEY")
        if alchemy_key:
            try:
                # Alchemy supports multiple networks via subdomain
                url_map = {
                    "ethereum": f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}",
                    "polygon": f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}",
                    "bsc": f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}" 
                }
                url = url_map.get(network.lower(), url_map["ethereum"])
                
                payload = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "eth_getCode",
                    "params": [ca, "latest"]
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, timeout=5) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            code = data.get("result", "0x")
                            # If result is not "0x", it's a contract (not a wallet)
                            if code != "0x":
                                result["is_contract"] = True
                                result["verified"] = True
                                logger.info(f"✅ Verified CA {ca} via Alchemy ({network})")
            except Exception as e:
                logger.warning(f"Alchemy verification failed: {e}")

        # 2. Etherscan Fallback (Public Records)
        etherscan_key = os.environ.get("ETHERSCAN_API_KEY")
        if not result["verified"] and etherscan_key:
            try:
                url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={ca}&apikey={etherscan_key}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=5) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status") == "1" and data.get("result"):
                                contract_name = data["result"][0].get("ContractName")
                                if contract_name:
                                    result["is_contract"] = True
                                    result["name"] = contract_name
                                    result["verified"] = True
                                    logger.info(f"✅ Verified CA {ca} via Etherscan: {contract_name}")
            except Exception as e:
                logger.warning(f"Etherscan verification failed: {e}")

        return result
