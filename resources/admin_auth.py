import argparse

from rest_tools.server import Auth


def main():
    parser = argparse.ArgumentParser(description='Generates a token for pyglidein server. Admin token by default, unless specifying --client')
    parser.add_argument('secret', help='auth secret')
    parser.add_argument('-e', '--expiration', help='token expiration (in seconds)')
    parser.add_argument('--client', help='client name for generating a client token')
    args = parser.parse_args()

    kwargs = {}
    if args.expiration:
        kwargs['expiration'] = args.expiration
    a = Auth(args.secret, issuer='pyglidein', **kwargs)

    if args.client:
        token = a.create_token(args.client, type='client', payload={'role': 'client'})
        print('Client token:')
        print(token)
    else:
        token = a.create_token('admin', type='client', payload={'role': 'admin'})
        print('Admin token:')
        print(token)


if __name__ == '__main__':
    main()
