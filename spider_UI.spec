# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['spider_UI.py', 'deny_access.py', 'file_path.py'],
             pathex=['D:\\Pyproject\\Pyzjex', 'D:\\Pyproject\\Pyzjex\\qs_spider'],
             binaries=[],
             datas=[],
             hiddenimports=['deny_access', 'file_path'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='券商信息采集器',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=False , icon='zjexico1.ico')
