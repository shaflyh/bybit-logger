#!/usr/bin/env python3
"""Debug script to test FUND wallet balance endpoint with price enrichment"""

from bybit_service import BybitService
from data_processor import DataProcessor
import json


def main():
    print("=" * 50)
    print("FUND Wallet Balance Debug Script")
    print("=" * 50)

    # Initialize service
    service = BybitService()

    # Test 1: FUND wallet with automatic price enrichment
    print("\n1. Testing FUND wallet balance (with automatic USD value enrichment)...")
    print("-" * 50)
    fund_result = service.get_funding_wallet_balance()

    if fund_result:
        print("\n✅ FUND Wallet fetched successfully!")
        balance_list = fund_result.get('balance', [])

        print(f"\nFound {len(balance_list)} coins in FUND wallet:")
        total_usd = 0
        for coin in balance_list:
            wallet_bal = float(coin.get('walletBalance', 0))
            usd_val = float(coin.get('usdValue', 0))
            if wallet_bal > 0:
                total_usd += usd_val
                print(f"   • {coin.get('coin')}: {wallet_bal} (≈ ${usd_val:.2f})")

        print(f"\n   Total FUND wallet value: ${total_usd:.2f}")
    else:
        print("\n❌ Failed to fetch FUND wallet")

    # Test 2: UNIFIED wallet for comparison
    print("\n" + "=" * 50)
    print("\n2. Testing UNIFIED wallet balance...")
    print("-" * 50)
    unified_result = service.get_wallet_balance(account_type="UNIFIED")

    if unified_result:
        total_equity = float(unified_result['list'][0].get('totalEquity', 0))
        print(f"\n✅ UNIFIED wallet fetched successfully!")
        print(f"   Total equity: ${total_equity:.2f}")
    else:
        print("\n❌ Failed to fetch UNIFIED wallet")

    # Test 3: Asset allocation processing
    print("\n" + "=" * 50)
    print("\n3. Testing asset allocation data processing...")
    print("-" * 50)

    if fund_result and unified_result:
        asset_allocation = DataProcessor.process_asset_allocation(
            unified_result, fund_result
        )

        print(f"\n✅ Asset Allocation processed successfully!")
        print(f"\nTop assets by USD value:")
        print(f"{'Coin':<10} {'Wallet':<10} {'Balance':<20} {'USD Value':<15} {'%':<10}")
        print("-" * 75)

        for asset in asset_allocation[:10]:  # Show top 10
            coin = asset['Coin']
            wallet = asset['Wallet']
            balance = asset['Balance']
            usd_value = asset['USD Value']
            percentage = asset['Percentage']
            print(f"{coin:<10} {wallet:<10} {balance:<20} {usd_value:<15} {percentage:<10}")

        if len(asset_allocation) > 10:
            print(f"\n... and {len(asset_allocation) - 10} more assets")
    else:
        print("\n❌ Could not test asset allocation - missing wallet data")

    # Test 4: Check logged files
    print("\n" + "=" * 50)
    print("\n4. Logged files:")
    print("-" * 50)
    print("   📁 logs/wallet_balance_fund.json - FUND wallet with enriched USD values")
    print("   📁 logs/wallet_balance_unified.json - UNIFIED wallet")

    print("\n" + "=" * 50)
    print("✅ Debug script completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
