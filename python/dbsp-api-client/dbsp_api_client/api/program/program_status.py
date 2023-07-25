from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.program_descr import ProgramDescr
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Dict[str, Any]:
    pass

    params: Dict[str, Any] = {}
    params["id"] = id

    params["name"] = name

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    return {
        "method": "get",
        "url": "/program",
        "params": params,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, ProgramDescr]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ProgramDescr.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.BAD_REQUEST:
        response_400 = ErrorResponse.from_dict(response.json())

        return response_400
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ErrorResponse, ProgramDescr]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Response[Union[ErrorResponse, ProgramDescr]]:
    """Returns program descriptor, including current program version and

     Returns program descriptor, including current program version and
    compilation status.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, ProgramDescr]]
    """

    kwargs = _get_kwargs(
        id=id,
        name=name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Optional[Union[ErrorResponse, ProgramDescr]]:
    """Returns program descriptor, including current program version and

     Returns program descriptor, including current program version and
    compilation status.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, ProgramDescr]
    """

    return sync_detailed(
        client=client,
        id=id,
        name=name,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Response[Union[ErrorResponse, ProgramDescr]]:
    """Returns program descriptor, including current program version and

     Returns program descriptor, including current program version and
    compilation status.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, ProgramDescr]]
    """

    kwargs = _get_kwargs(
        id=id,
        name=name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Optional[Union[ErrorResponse, ProgramDescr]]:
    """Returns program descriptor, including current program version and

     Returns program descriptor, including current program version and
    compilation status.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, ProgramDescr]
    """

    return (
        await asyncio_detailed(
            client=client,
            id=id,
            name=name,
        )
    ).parsed
