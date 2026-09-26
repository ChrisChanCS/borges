#!/bin/bash

# Private key path
PRIVATE_KEY=~/.ssh/id_rsa_hw

# Target port range
START_PORT=10022
END_PORT=10029

# Public key to add
PUBKEY="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCfkppie+2WAjBrgWMuhLNNio6is1Ne1rqnJXDFp3P5mMbDRYPpcl5FIhzf1dU0V1nRWzTIxqVPNYzmFmjPa8urKxPJT8gSpBh/8NEuPEHy07Vox45YQkRbNF+GGZTeJWYGXWXBBUdsAQINwut2ZcpVvzikbsL11M0vGBXP0WhsYCA+GJSb84lCtq2+YT6rmAwrBUWdyEwZzHtgxs5VarrES8KJK68IINCDcrLGq/itXANNXotjxGsdRiDCsS5cpd2IHb7wmPwk2x63zl2GGYWbypwVeMPOaBA6Kt+Ra4EAembntMQ14hSlP7o7OcE5zXpi1yWxPGfxyD1p+/HXRELSgc/RF3Vn+reAAZZpCGXZcrbF+JrGm0SxhlkTAUjwAk1a7Oc8thr10TakoM2QmBrFO0c4ZZ9zXIbwplY1MvsVy4jemd9l8WdSUT5zgL5RysCTwqY6ucYkH/6gNvAjWMCB3HmB5wRgtz9FqDXMjrLB+blp05lXbZaUoi1k3IdqS50= uta@janux-SYS-212H-TN"

for PORT in $(seq $START_PORT $END_PORT); do
    echo "🔐 Connecting to 127.0.0.1:$PORT ..."

    ssh -i "$PRIVATE_KEY" -p "$PORT" -o StrictHostKeyChecking=no -o ConnectTimeout=5 root@127.0.0.1 bash <<EOF
mkdir -p ~/.ssh
touch ~/.ssh/authorized_keys
grep -qF "$PUBKEY" ~/.ssh/authorized_keys || echo "$PUBKEY" >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
echo "✅ Added public key to port $PORT"
EOF

    if [ $? -ne 0 ]; then
        echo "❌ Connection or key installation failed: 127.0.0.1:$PORT"
    fi
done
